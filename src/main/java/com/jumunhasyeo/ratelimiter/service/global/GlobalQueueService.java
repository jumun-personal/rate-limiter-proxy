package com.jumunhasyeo.ratelimiter.service.global;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jumunhasyeo.ratelimiter.domain.QueueItem;
import com.jumunhasyeo.ratelimiter.domain.QueuePollResult;
import com.jumunhasyeo.ratelimiter.properties.QueueWeightProperties;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
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
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class GlobalQueueService {

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final GlobalRateLimiterService rateLimiterService;

    // 전역 대기열 Redis 키
    private static final String GLOBAL_ORDER_KEY = "queue:global:order";
    private static final String GLOBAL_ORDER_RETRY_KEY = "queue:global:order:retry";
    private static final String GLOBAL_OTHER_KEY = "queue:global:other";
    private static final String GLOBAL_OTHER_RETRY_KEY = "queue:global:other:retry";

    // PG 대기열 Redis 키
    private static final String PG_ORDER_KEY = "queue:pg:order";
    private static final String PG_ORDER_RETRY_KEY = "queue:pg:order:retry";
    private static final String PG_OTHER_KEY = "queue:pg:other";
    private static final String PG_OTHER_RETRY_KEY = "queue:pg:other:retry";

    // 재시도 대기 시간 (ms)
    private static final long RETRY_DELAY_MS = 4000;

    @Value("classpath:scripts/queue_weighting.lua")
    private Resource queueWeightingScriptResource;

    @Value("classpath:scripts/queue_poll_only.lua")
    private Resource queuePollOnlyScriptResource;

    private RedisScript<String> weightedPollScript;
    private RedisScript<String> weightedPollOnlyScript;

    @PostConstruct
    public void init() throws IOException {
        if (queueWeightingScriptResource.exists()) {
            String scriptContent = queueWeightingScriptResource.getContentAsString(StandardCharsets.UTF_8);
            weightedPollScript = RedisScript.of(scriptContent, String.class);
            log.debug("queue_weighting.lua script 로드 완료");
        } else {
            log.warn("queue_weighting.lua script 파일 없음 (classpath)");
        }
        weightedPollOnlyScript = RedisScript.of(
                queuePollOnlyScriptResource.getContentAsString(StandardCharsets.UTF_8), String.class);
        log.debug("GlobalQueueService: Lua script 파일 로드 완료");
    }

    @Getter
    public enum QueueType {
        ORDER(GLOBAL_ORDER_KEY, GLOBAL_ORDER_RETRY_KEY),
        OTHER(GLOBAL_OTHER_KEY, GLOBAL_OTHER_RETRY_KEY);

        private final String key;
        private final String retryKey;

        QueueType(String key, String retryKey) {
            this.key = key;
            this.retryKey = retryKey;
        }
    }

    public QueuePollResult pollWeightedPg(int totalSlots, QueueWeightProperties props) {
        List<String> keys = List.of(
                PG_ORDER_KEY,
                PG_ORDER_RETRY_KEY,
                PG_OTHER_KEY,
                PG_OTHER_RETRY_KEY
        );
        return pollWeightedInternal(keys, totalSlots, props);
    }

    public QueuePollResult pollWeightedGlobalOnly(int totalSlots, QueueWeightProperties props) {
        List<String> keys = List.of(
                GLOBAL_ORDER_KEY,
                GLOBAL_ORDER_RETRY_KEY,
                GLOBAL_OTHER_KEY,
                GLOBAL_OTHER_RETRY_KEY
        );
        return pollWeightedInternal(keys, totalSlots, props);
    }

    private QueuePollResult pollWeightedInternal(List<String> keys, int totalSlots, QueueWeightProperties props) {
        if (totalSlots <= 0) return QueuePollResult.empty();

        long now = System.currentTimeMillis();
        long retryThreshold = now - props.getRetryDelayMs();

        try {
            String result = redisTemplate.execute(
                    weightedPollOnlyScript,
                    keys,
                    String.valueOf(now),
                    String.valueOf(totalSlots),
                    String.valueOf(props.getOrder()),
                    String.valueOf(props.getOther()),
                    String.valueOf(props.getRetryRatio()),
                    String.valueOf(retryThreshold)
            );
            return parsePollResultSafely(result);
        } catch (Exception e) {
            log.error("pollWeightedInternal 오류 발생", e);
            return QueuePollResult.empty();
        }
    }

    private QueuePollResult parsePollResultSafely(String json) {
        if (json == null || json.isEmpty()) {
            return QueuePollResult.empty();
        }
        try {
            QueuePollResult result = objectMapper.readValue(json, QueuePollResult.class);
            if (result == null) return QueuePollResult.empty();
            if (result.getItems() == null) result.setItems(Collections.emptyList());
            if (result.getStats() == null) result.setStats(new QueuePollResult.QueueStats(0, 0, 0, 0, 0, 0));
            if (result.getBucket() == null) result.setBucket(new QueuePollResult.BucketState(0.0, 0));
            return result;
        } catch (Exception e) {
            log.error("Poll 결과 파싱 오류 raw={}", json, e);
            return QueuePollResult.empty();
        }
    }

    public QueueType resolveQueueType(String method, String uri) {
        if (uri != null && uri.contains("/orders") && "POST".equalsIgnoreCase(method)) {
            return QueueType.ORDER;
        }
        return QueueType.OTHER;
    }

    public boolean offer(QueueItem item, QueueType queueType) {
        try {
            String value = objectMapper.writeValueAsString(item);
            double score = item.getOriginalTimestamp();
            Boolean added = redisTemplate.opsForZSet().add(queueType.getKey(), value, score);
            log.debug("Queue offer [{}]: 결과={}", queueType, added);
            return added != null && added;
        } catch (JsonProcessingException e) {
            log.error("QueueItem 직렬화 실패", e);
            return false;
        }
    }

    public boolean offerToRetry(QueueItem item, QueueType queueType) {
        try {
            String value = objectMapper.writeValueAsString(item);
            double score = System.currentTimeMillis();
            Boolean added = redisTemplate.opsForZSet().add(queueType.getRetryKey(), value, score);
            log.debug("Retry Queue offer [{}]: 결과={}", queueType, added);
            return added != null && added;
        } catch (JsonProcessingException e) {
            log.error("Retry용 QueueItem 직렬화 실패", e);
            return false;
        }
    }

    public List<QueueItem> poll(QueueType queueType, int size) {
        return pollFromKey(queueType.getKey(), size);
    }

    public List<QueueItem> pollFromRetry(QueueType queueType, int size) {
        return pollFromKey(queueType.getRetryKey(), size);
    }

    public List<QueueItem> pollRetryEligible(QueueType queueType, int size) {
        long threshold = System.currentTimeMillis() - RETRY_DELAY_MS;

        Set<String> items = redisTemplate.opsForZSet()
                .rangeByScore(queueType.getRetryKey(), Double.NEGATIVE_INFINITY, threshold, 0, size);

        if (items == null || items.isEmpty()) {
            return Collections.emptyList();
        }

        List<QueueItem> result = items.stream()
                .map(this::deserialize)
                .collect(Collectors.toList());

        redisTemplate.opsForZSet().remove(queueType.getRetryKey(), items.toArray());

        return result;
    }

    private List<QueueItem> pollFromKey(String key, int size) {
        if (size <= 0) {
            return Collections.emptyList();
        }

        Set<String> items = redisTemplate.opsForZSet().range(key, 0, size - 1);

        if (items == null || items.isEmpty()) {
            return Collections.emptyList();
        }

        List<QueueItem> result = items.stream()
                .map(this::deserialize)
                .collect(Collectors.toList());

        redisTemplate.opsForZSet().remove(key, items.toArray());

        return result;
    }

    public Long findSequence(Long userId, QueueType queueType) {
        Set<String> allItems = redisTemplate.opsForZSet().range(queueType.getKey(), 0, -1);

        if (allItems == null) {
            return -1L;
        }

        long position = 0;
        for (String item : allItems) {
            QueueItem queueItem = deserialize(item);
            if (queueItem.getUserId().equals(userId)) {
                return position;
            }
            position++;
        }
        return -1L;
    }

    public Long getQueueSize(QueueType queueType) {
        Long size = redisTemplate.opsForZSet().size(queueType.getKey());
        return size != null ? size : 0L;
    }

    public Long getRetryQueueSize(QueueType queueType) {
        Long size = redisTemplate.opsForZSet().size(queueType.getRetryKey());
        return size != null ? size : 0L;
    }

    public Long getRetryEligibleCount(QueueType queueType) {
        long threshold = System.currentTimeMillis() - RETRY_DELAY_MS;
        Long count = redisTemplate.opsForZSet().count(queueType.getRetryKey(), Double.NEGATIVE_INFINITY, threshold);
        return count != null ? count : 0L;
    }

    public Long getTotalQueueSize() {
        return getQueueSize(QueueType.ORDER) + getQueueSize(QueueType.OTHER);
    }

    public Long getTotalRetryQueueSize() {
        return getRetryQueueSize(QueueType.ORDER) + getRetryQueueSize(QueueType.OTHER);
    }

    private QueueItem deserialize(String json) {
        try {
            return objectMapper.readValue(json, QueueItem.class);
        } catch (JsonProcessingException e) {
            log.error("QueueItem 역직렬화 실패: {}", json, e);
            throw new RuntimeException(e);
        }
    }

    public QueuePollResult pollWeighted(int totalSlots, QueueWeightProperties config) {
        if (weightedPollScript == null) {
            log.warn("Weighted Poll Script 미로드, 빈 결과 반환");
            return QueuePollResult.empty();
        }

        if (totalSlots <= 0) {
            return QueuePollResult.empty();
        }

        long now = System.currentTimeMillis();
        long retryThreshold = now - RETRY_DELAY_MS;

        List<String> keys = Arrays.asList(
                GLOBAL_ORDER_KEY,
                GLOBAL_ORDER_RETRY_KEY,
                GLOBAL_OTHER_KEY,
                GLOBAL_OTHER_RETRY_KEY,
                "leaky:global:bucket"
        );

        try {
            String result = redisTemplate.execute(
                    weightedPollScript,
                    keys,
                    String.valueOf(now),
                    String.valueOf(totalSlots),
                    String.valueOf(config.getOrder()),
                    String.valueOf(config.getOther()),
                    String.valueOf(config.getRetryRatio()),
                    String.valueOf(retryThreshold),
                    String.valueOf(rateLimiterService.getCurrentLimit()),
                    String.valueOf(rateLimiterService.getCurrentLimit())
            );
            return parseQueuePollResult(result);
        } catch (Exception e) {
            log.error("Weighted Poll 실패", e);
            return QueuePollResult.empty();
        }
    }

    private QueuePollResult parseQueuePollResult(String json) {
        if (json == null || json.isEmpty()) {
            return QueuePollResult.empty();
        }
        try {
            JsonNode root = objectMapper.readTree(json);

            List<QueuePollResult.QueuePollItem> items = new ArrayList<>();
            JsonNode itemsNode = root.get("items");
            if (itemsNode != null && itemsNode.isArray()) {
                for (JsonNode itemNode : itemsNode) {
                    QueuePollResult.QueuePollItem item = new QueuePollResult.QueuePollItem(
                            itemNode.get("queue").asText(),
                            itemNode.get("data").asText(),
                            itemNode.get("score").asLong()
                    );
                    items.add(item);
                }
            }

            JsonNode statsNode = root.get("stats");
            QueuePollResult.QueueStats stats = new QueuePollResult.QueueStats(
                    statsNode.get("order_retry").asInt(),
                    statsNode.get("order_normal").asInt(),
                    statsNode.get("other_retry").asInt(),
                    statsNode.get("other_normal").asInt(),
                    statsNode.get("total_polled").asInt(),
                    statsNode.get("remaining_slots").asInt()
            );

            JsonNode bucketNode = root.get("bucket");
            QueuePollResult.BucketState bucket = new QueuePollResult.BucketState(
                    bucketNode.get("water_level").asDouble(),
                    bucketNode.get("tokens_consumed").asInt()
            );

            return new QueuePollResult(items, stats, bucket);
        } catch (Exception e) {
            log.error("Lua Script 결과 파싱 실패: {}", json, e);
            return QueuePollResult.empty();
        }
    }

    public boolean isWeightedPollAvailable() {
        return weightedPollScript != null;
    }
}
