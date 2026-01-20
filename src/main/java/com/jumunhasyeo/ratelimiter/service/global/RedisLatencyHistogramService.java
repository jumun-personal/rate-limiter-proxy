package com.jumunhasyeo.ratelimiter.service.global;

import com.jumunhasyeo.ratelimiter.properties.FeedbackLoopProperties;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class RedisLatencyHistogramService {

    private final StringRedisTemplate redisTemplate;
    private final FeedbackLoopProperties properties;

    private static final String HISTOGRAM_KEY_PREFIX = "latency:histogram:";

    @Value("classpath:scripts/record_latency.lua")
    private Resource recordLatencyScriptResource;

    private RedisScript<Long> recordLatencyScript;

    @PostConstruct
    public void init() throws IOException {
        recordLatencyScript = RedisScript.of(
                recordLatencyScriptResource.getContentAsString(StandardCharsets.UTF_8), Long.class);
        log.debug("RedisLatencyHistogramService: Lua script 파일 로드 완료");
    }

    public void recordLatency(long latencyMs) {
        var histogramParams = properties.getHistogram();
        long timeSlice = getTimeSlice(histogramParams.getTimeSliceDurationMs());
        String histogramKey = HISTOGRAM_KEY_PREFIX + timeSlice;

        int[] boundaries = histogramParams.getBucketBoundaries();
        int ttlSeconds = (histogramParams.getTimeSliceDurationMs() * histogramParams.getMaxSlices()) / 1000 + 10;

        List<String> args = new ArrayList<>();
        args.add(String.valueOf(latencyMs));
        args.add(String.valueOf(ttlSeconds));
        args.add(String.valueOf(boundaries.length));
        for (int boundary : boundaries) {
            args.add(String.valueOf(boundary));
        }

        try {
            redisTemplate.execute(
                    recordLatencyScript,
                    Collections.singletonList(histogramKey),
                    args.toArray(new String[0])
            );
        } catch (Exception e) {
            log.warn("Latency 기록 실패: {}", e.getMessage());
        }
    }

    public double getP95Latency() {
        return calculatePercentile(0.95);
    }

    public double getP99Latency() {
        return calculatePercentile(0.99);
    }

    private double calculatePercentile(double percentile) {
        Map<Integer, Long> histogram = aggregateRecentHistograms();

        if (histogram.isEmpty()) {
            log.debug("P{} Histogram 데이터 없음", (int) (percentile * 100));
            return 0.0;
        }

        int[] boundaries = properties.getHistogram().getBucketBoundaries();

        long totalCount = histogram.getOrDefault(boundaries[boundaries.length - 1], 0L);

        if (totalCount == 0) {
            return 0.0;
        }

        long targetCount = (long) Math.ceil(totalCount * percentile);

        long prevCount = 0;
        int prevBoundary = 0;

        for (int boundary : boundaries) {
            long cumulativeCount = histogram.getOrDefault(boundary, 0L);

            if (cumulativeCount >= targetCount) {
                if (cumulativeCount == prevCount) {
                    return (double) boundary;
                }

                double fraction = (double) (targetCount - prevCount) /
                        (cumulativeCount - prevCount);
                return prevBoundary + fraction * (boundary - prevBoundary);
            }

            prevCount = cumulativeCount;
            prevBoundary = boundary;
        }

        return (double) boundaries[boundaries.length - 1];
    }

    private Map<Integer, Long> aggregateRecentHistograms() {
        var histogramParams = properties.getHistogram();
        int timeSliceDurationMs = histogramParams.getTimeSliceDurationMs();
        int maxSlices = histogramParams.getMaxSlices();

        long currentSlice = getTimeSlice(timeSliceDurationMs);

        List<Map<Integer, Long>> histogramList = new ArrayList<>();

        for (int i = 0; i < maxSlices; i++) {
            String key = HISTOGRAM_KEY_PREFIX + (currentSlice - (long) i * timeSliceDurationMs);
            try {
                Map<Object, Object> entries = redisTemplate.opsForHash().entries(key);
                Map<Integer, Long> histogram = new HashMap<>();
                for (Map.Entry<Object, Object> entry : entries.entrySet()) {
                    histogram.put(
                            Integer.parseInt(entry.getKey().toString()),
                            Long.parseLong(entry.getValue().toString())
                    );
                }
                histogramList.add(histogram);
            } catch (Exception e) {
                log.debug("Histogram 조회 실패 key: {}", key);
            }
        }

        return mergeHistograms(histogramList);
    }

    private Map<Integer, Long> mergeHistograms(List<Map<Integer, Long>> histogramList) {
        Map<Integer, Long> merged = new HashMap<>();

        for (Map<Integer, Long> histogram : histogramList) {
            for (Map.Entry<Integer, Long> entry : histogram.entrySet()) {
                merged.merge(entry.getKey(), entry.getValue(), Long::sum);
            }
        }

        return merged;
    }

    private long getTimeSlice(int durationMs) {
        return (System.currentTimeMillis() / durationMs) * durationMs;
    }

    public void clearHistograms() {
        var histogramParams = properties.getHistogram();
        int timeSliceDurationMs = histogramParams.getTimeSliceDurationMs();
        int maxSlices = histogramParams.getMaxSlices();
        long currentSlice = getTimeSlice(timeSliceDurationMs);

        for (int i = 0; i < maxSlices; i++) {
            String key = HISTOGRAM_KEY_PREFIX + (currentSlice - (long) i * timeSliceDurationMs);
            try {
                redisTemplate.delete(key);
            } catch (Exception e) {
                log.debug("Histogram 삭제 실패 key: {}", key);
            }
        }
    }
}
