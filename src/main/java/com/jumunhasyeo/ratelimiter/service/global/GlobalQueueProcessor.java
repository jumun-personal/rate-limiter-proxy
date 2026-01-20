package com.jumunhasyeo.ratelimiter.service.global;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jumunhasyeo.ratelimiter.domain.HttpRequestData;
import com.jumunhasyeo.ratelimiter.domain.QueueItem;
import com.jumunhasyeo.ratelimiter.domain.QueuePollResult;
import com.jumunhasyeo.ratelimiter.properties.QueueWeightProperties;
import com.jumunhasyeo.ratelimiter.service.global.GlobalQueueService.QueueType;
import com.jumunhasyeo.ratelimiter.service.pg.RateLimiterService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Component
@RequiredArgsConstructor
public class GlobalQueueProcessor {

    private final GlobalQueueService globalQueueService;
    private final GlobalRateLimiterService globalRateLimiterService;
    private final RateLimiterService pgRateLimiterService;
    private final QueueWeightProperties weightProperties;
    private final RestClient orderServiceRestClient;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;
    private final RedisLatencyHistogramService histogramService;
    private final FeedbackLoopStateManager stateManager;

    @Value("${queue.use-lua-polling:true}")
    private boolean useLuaPolling;

    @Value("${queue.retry.max-retry-count:1}")
    private int maxRetryCount;

    private static final String DEFAULT_PROVIDER = "TOSS";

    @Scheduled(fixedDelayString = "${queue.processor-interval-ms:100}")
    public void processQueue() {
        try {
            if (useLuaPolling && globalQueueService.isWeightedPollAvailable()) {
                processWithLuaScript();
            } else {
                processWithJava();
            }
        } catch (Exception e) {
            log.error("Queue 처리 오류", e);
        }
    }

    private void processWithLuaScript() {
        int desired = globalRateLimiterService.getCurrentLimit();

        // PG 레인: PG + 전역 토큰 모두 필요
        long pgReserved = pgRateLimiterService.tryConsumeN(DEFAULT_PROVIDER, desired);
        if (pgReserved > 0) {
            long globalReserved = globalRateLimiterService.tryConsumeNForQueue(pgReserved);
            int allowed = (int) Math.min(pgReserved, globalReserved);

            // pgReserved > globalReserved인 경우 차이만큼 PG 토큰 반환 (토큰 누수 방지)
            if (pgReserved > globalReserved) {
                pgRateLimiterService.refundN(DEFAULT_PROVIDER, pgReserved - globalReserved);
            }

            if (allowed <= 0) {
                pgRateLimiterService.refundN(DEFAULT_PROVIDER, globalReserved);
            } else {
                QueuePollResult result = globalQueueService.pollWeightedPg(allowed, weightProperties);
                refundIfShortAndProcess(result, allowed, true);
            }
        }

        // 전역 전용 레인: 전역 토큰만 필요
        int remain = Math.max(0, desired - (int) pgReserved);
        if (remain > 0) {
            long globalAllowed = globalRateLimiterService.tryConsumeNForQueue(remain);
            if (globalAllowed > 0) {
                QueuePollResult result = globalQueueService.pollWeightedGlobalOnly((int) globalAllowed, weightProperties);
                refundIfShortAndProcess(result, (int) globalAllowed, false);
            }
        }
    }

    private void refundIfShortAndProcess(QueuePollResult result, int allowed, boolean isPgLane) {
        int polled = result.getStats().getTotalPolled();
        int refund = allowed - polled;

        if (refund > 0) {
            if (isPgLane) {
                pgRateLimiterService.refundN(DEFAULT_PROVIDER, refund);
                globalRateLimiterService.refundNForQueue(refund);
            } else {
                globalRateLimiterService.refundNForQueue(refund);
            }
        }

        processPolledItems(result);
    }

    private void processPolledItems(QueuePollResult result) {
        if (result.getItems().isEmpty()) {
            return;
        }

        log.debug("{} 건 처리 중 (Lua) - ORDER: retry={}, normal={} | OTHER: retry={}, normal={}",
                result.getStats().getTotalPolled(),
                result.getStats().getOrderRetry(),
                result.getStats().getOrderNormal(),
                result.getStats().getOtherRetry(),
                result.getStats().getOtherNormal());

        for (QueuePollResult.QueuePollItem item : result.getItems()) {
            QueueItem queueItem = deserializeQueueItem(item.getData());
            if (queueItem == null) {
                continue;
            }
            QueueType queueType = item.getQueue().startsWith("order") ?
                    QueueType.ORDER : QueueType.OTHER;
            boolean isRetry = item.getQueue().endsWith("retry");

            executeRequest(queueItem, queueType, isRetry);
        }
    }

    private QueueItem deserializeQueueItem(String json) {
        try {
            return objectMapper.readValue(json, QueueItem.class);
        } catch (JsonProcessingException e) {
            log.error("QueueItem 역직렬화 실패: {}", json, e);
            return null;
        }
    }

    private void processWithJava() {
        long pgTokens = pgRateLimiterService.getAvailableTokens(DEFAULT_PROVIDER);
        if (pgTokens <= 0) return;

        long globalTokens = globalRateLimiterService.getAvailableTokens();
        int availableSlots = (int) Math.min(pgTokens, globalTokens);
        if (availableSlots <= 0) return;

        processWithDynamicWeight(availableSlots);
    }

    private void processWithDynamicWeight(int availableSlots) {
        int orderQueueSize = globalQueueService.getQueueSize(QueueType.ORDER).intValue();
        int otherQueueSize = globalQueueService.getQueueSize(QueueType.OTHER).intValue();
        int orderRetrySize = globalQueueService.getRetryEligibleCount(QueueType.ORDER).intValue();
        int otherRetrySize = globalQueueService.getRetryEligibleCount(QueueType.OTHER).intValue();

        int[] slots = calculateSlots(availableSlots,
                orderQueueSize + orderRetrySize,
                otherQueueSize + otherRetrySize);
        int orderSlots = slots[0];
        int otherSlots = slots[1];

        int orderRetrySlots = Math.min(orderSlots, orderRetrySize);
        int orderNormalSlots = orderSlots - orderRetrySlots;

        int otherRetrySlots = Math.min(otherSlots, otherRetrySize);
        int otherNormalSlots = otherSlots - otherRetrySlots;

        log.debug("처리 중 - ORDER: retry={}, normal={} | OTHER: retry={}, normal={}",
                orderRetrySlots, orderNormalSlots, otherRetrySlots, otherNormalSlots);

        processQueueType(QueueType.ORDER, orderRetrySlots, orderNormalSlots);
        processQueueType(QueueType.OTHER, otherRetrySlots, otherNormalSlots);
    }

    private void processQueueType(QueueType queueType, int retrySlots, int normalSlots) {
        processRetryQueue(queueType, retrySlots);
        processNormalQueue(queueType, normalSlots);
    }

    private void processRetryQueue(QueueType queueType, int count) {
        if (count <= 0) return;

        List<QueueItem> items = globalQueueService.pollRetryEligible(queueType, count);
        for (QueueItem item : items) {
            processItem(item, queueType, true);
        }
    }

    private void processNormalQueue(QueueType queueType, int count) {
        if (count <= 0) return;

        List<QueueItem> items = globalQueueService.poll(queueType, count);
        for (QueueItem item : items) {
            processItem(item, queueType, false);
        }
    }

    private int[] calculateSlots(int availableSlots, int orderTotal, int otherTotal) {
        if (orderTotal == 0 && otherTotal == 0) {
            return new int[]{0, 0};
        }
        if (orderTotal == 0) {
            return new int[]{0, Math.min(availableSlots, otherTotal)};
        }
        if (otherTotal == 0) {
            return new int[]{Math.min(availableSlots, orderTotal), 0};
        }

        int orderWeight = weightProperties.getOrder();
        int otherWeight = weightProperties.getOther();
        int totalWeight = orderWeight + otherWeight;

        int baseOrderSlots = (int) Math.ceil(availableSlots * (double) orderWeight / totalWeight);
        int baseOtherSlots = availableSlots - baseOrderSlots;

        int actualOrderSlots = Math.min(baseOrderSlots, orderTotal);
        int actualOtherSlots = Math.min(baseOtherSlots, otherTotal);

        int remainingSlots = availableSlots - actualOrderSlots - actualOtherSlots;

        if (remainingSlots > 0) {
            int orderCanTakeMore = orderTotal - actualOrderSlots;
            int otherCanTakeMore = otherTotal - actualOtherSlots;

            if (orderCanTakeMore > 0) {
                int extra = Math.min(remainingSlots, orderCanTakeMore);
                actualOrderSlots += extra;
                remainingSlots -= extra;
            }
            if (remainingSlots > 0 && otherCanTakeMore > 0) {
                int extra = Math.min(remainingSlots, otherCanTakeMore);
                actualOtherSlots += extra;
            }
        }

        return new int[]{actualOrderSlots, actualOtherSlots};
    }

    private void processItem(QueueItem item, QueueType queueType, boolean isRetry) {
        GlobalRateLimiterService.TryConsumeResult result = globalRateLimiterService.tryConsume(false);
        if (result != GlobalRateLimiterService.TryConsumeResult.ALLOWED) {
            log.debug("Global Token 소진 (result={}), 재대기열 추가 userId={}", result, item.getUserId());
            requeue(item, queueType, isRetry);
            return;
        }

        boolean pgOk = pgRateLimiterService.tryConsume(DEFAULT_PROVIDER);
        if (!pgOk) {
            log.debug("PG Token 소진, 재대기열 추가 userId={}", item.getUserId());
            requeue(item, queueType, isRetry);
            return;
        }

        executeRequest(item, queueType, isRetry);
    }

    private void requeue(QueueItem item, QueueType queueType, boolean wasRetry) {
        if (wasRetry) {
            globalQueueService.offerToRetry(item, queueType);
        } else {
            globalQueueService.offer(item, queueType);
        }
    }

    private void recordWaitTime(long enqueueTimestamp, QueueType queueType, boolean isRetry) {
        long waitTimeMs = System.currentTimeMillis() - enqueueTimestamp;
        Timer.builder("queue.wait.time")
                .description("대기열에서 처리까지 소요된 시간")
                .tag("queue_type", queueType.name())
                .tag("retry", String.valueOf(isRetry))
                .register(meterRegistry)
                .record(waitTimeMs, TimeUnit.MILLISECONDS);
    }

    private void recordRetryResult(boolean success, QueueType queueType) {
        Counter.builder("queue.retry.result")
                .description("재시도 요청 결과")
                .tag("queue_type", queueType.name())
                .tag("success", String.valueOf(success))
                .register(meterRegistry)
                .increment();
    }

    private void executeRequest(QueueItem item, QueueType queueType, boolean isRetry) {
        recordWaitTime(item.getOriginalTimestamp(), queueType, isRetry);

        HttpRequestData request = item.getHttpRequest();
        if (request == null) {
            log.warn("유효하지 않은 요청 데이터 userId={}", item.getUserId());
            return;
        }

        String path = extractPath(request.getUri());
        URI original = URI.create(request.getUri());
        long requestStartTime = System.currentTimeMillis();

        try {
            RestClient.RequestBodySpec requestSpec = orderServiceRestClient
                    .method(HttpMethod.valueOf(request.getMethod()))
                    .uri(uriBuilder -> uriBuilder
                            .path(original.getPath())
                            .query(original.getQuery())
                            .build());

            requestSpec.contentType(MediaType.APPLICATION_JSON);

            if (request.getHeaders() != null) {
                request.getHeaders().forEach(requestSpec::header);
            }
            if (item.getAccessToken() != null) {
                requestSpec.header("Authorization", "Bearer " + item.getAccessToken());
            }

            if (request.getBody() != null && !request.getBody().isEmpty()) {
                requestSpec.body(request.getBody());
            }

            String response = requestSpec.retrieve().body(String.class);

            // 피드백 루프 활성 시 지연 시간 기록
            if (stateManager.isActive()) {
                long latency = System.currentTimeMillis() - requestStartTime;
                histogramService.recordLatency(latency);
            }

            if (isRetry) {
                recordRetryResult(true, queueType);
            }
            log.debug("{} 요청 처리 완료 userId={}, isRetry={}",
                    DEFAULT_PROVIDER, item.getUserId(), isRetry);

        } catch (Exception e) {
            handleRequestError(e, item, queueType, isRetry);
        }
    }

    private void handleRequestError(Throwable e, QueueItem item, QueueType queueType, boolean isRetry) {
        String errorType = e.getClass().getSimpleName();
        String errorMsg = e.getMessage() != null ? e.getMessage() : "no message";
        log.error("요청 실패 userId={}, isRetry={}, error={}: {}",
                item.getUserId(), isRetry, errorType, errorMsg);

        if (isRetry) {
            recordRetryResult(false, queueType);
            log.error("Retry 실패, 요청 삭제 userId={}", item.getUserId());
            return;
        }

        if (isRetryable(e) && item.canRetry(maxRetryCount)) {
            item.incrementRetryCount();
            log.warn("재시도 가능한 오류 ({}), Retry Queue로 이동 userId={}", errorType, item.getUserId());
            globalQueueService.offerToRetry(item, queueType);
            return;
        }

        log.error("재시도 불가 오류 ({}), 요청 삭제 userId={}", errorType, item.getUserId());
    }

    private boolean isRetryable(Throwable e) {
        if (e instanceof TimeoutException) return true;
        if (e instanceof SocketTimeoutException) return true;
        if (e instanceof RestClientException && e.getMessage() != null
                && e.getMessage().contains("5")) return true;

        if (e.getCause() != null) {
            return isRetryable(e.getCause());
        }
        return false;
    }

    private String extractPath(String uri) {
        try {
            URI parsed = new URI(uri);
            String path = parsed.getPath();
            String query = parsed.getQuery();
            return query != null ? path + "?" + query : path;
        } catch (Exception e) {
            return uri;
        }
    }
}
