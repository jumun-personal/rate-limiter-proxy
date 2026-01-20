package com.jumunhasyeo.ratelimiter.metrics;

import com.jumunhasyeo.ratelimiter.service.global.GlobalQueueService;
import com.jumunhasyeo.ratelimiter.service.global.GlobalQueueService.QueueType;
import com.jumunhasyeo.ratelimiter.service.global.GlobalRateLimiterService;
import com.jumunhasyeo.ratelimiter.service.pg.PaymentProviderRateLimiter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
@RequiredArgsConstructor
public class AllQueueMetrics {

    private final GlobalQueueService globalQueueService;
    private final GlobalRateLimiterService globalRateLimiterService;
    private final List<PaymentProviderRateLimiter> paymentProviderRateLimiters;
    private final MeterRegistry meterRegistry;

    private final AtomicLong cachedGlobalOrderQueueSize = new AtomicLong(0);
    private final AtomicLong cachedGlobalOtherQueueSize = new AtomicLong(0);
    private final AtomicLong cachedOrderRetryQueueSize = new AtomicLong(0);
    private final AtomicLong cachedOtherRetryQueueSize = new AtomicLong(0);
    private final AtomicLong cachedGlobalWindowCount = new AtomicLong(0);
    private final Map<String, AtomicLong> cachedPgCurrentTokens = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        log.debug("Metrics 초기화 중 (PG Provider: {} 개)", paymentProviderRateLimiters.size());

        Gauge.builder("queue.waiting.users.global.order", cachedGlobalOrderQueueSize, AtomicLong::get)
                .description("Number of users waiting in global ORDER queue")
                .register(meterRegistry);

        Gauge.builder("queue.waiting.users.global.other", cachedGlobalOtherQueueSize, AtomicLong::get)
                .description("Number of users waiting in global OTHER queue")
                .register(meterRegistry);

        Gauge.builder("queue.retry.order", cachedOrderRetryQueueSize, AtomicLong::get)
                .description("Number of requests in ORDER retry queue")
                .register(meterRegistry);

        Gauge.builder("queue.retry.other", cachedOtherRetryQueueSize, AtomicLong::get)
                .description("Number of requests in OTHER retry queue")
                .register(meterRegistry);

        Gauge.builder("rate.limit.global.max", globalRateLimiterService,
                        GlobalRateLimiterService::getCurrentLimit)
                .description("Global rate limit (leaky bucket leak rate)")
                .register(meterRegistry);

        Gauge.builder("rate.limit.global.current", cachedGlobalWindowCount, AtomicLong::get)
                .description("Current water level in global leaky bucket")
                .register(meterRegistry);

        Gauge.builder("rate.limit.global.usage", this, metrics -> {
                    long current = cachedGlobalWindowCount.get();
                    int max = globalRateLimiterService.getCurrentLimit();
                    return max > 0 ? (double) current / max * 100 : 0;
                })
                .description("Global rate limit usage percentage")
                .baseUnit("percent")
                .register(meterRegistry);

        for (PaymentProviderRateLimiter rateLimiter : paymentProviderRateLimiters) {
            String provider = rateLimiter.getProviderName();

            Gauge.builder("rate.limit.pg." + provider + ".max", rateLimiter,
                            PaymentProviderRateLimiter::getRateLimit)
                    .description("PG rate limit (leaky bucket leak rate)")
                    .register(meterRegistry);

            cachedPgCurrentTokens.put(provider, new AtomicLong(0));

            Gauge.builder("rate.limit.pg." + provider + ".current",
                            cachedPgCurrentTokens.get(provider),
                            AtomicLong::get)
                    .description("PG current available capacity (leaky bucket)")
                    .register(meterRegistry);

            Gauge.builder("rate.limit.pg." + provider + ".usage", this, metrics -> {
                        long current = cachedPgCurrentTokens.get(provider).get();
                        int max = rateLimiter.getRateLimit();
                        return max > 0 ? (double) (max - current) / max * 100 : 0;
                    })
                    .description("PG rate limit usage percentage")
                    .baseUnit("percent")
                    .register(meterRegistry);
        }

        log.debug("모든 Metrics 등록 완료");
    }

    @Scheduled(fixedDelay = 5000, initialDelay = 2000)
    public void updateGlobalQueueSize() {
        try {
            cachedGlobalOrderQueueSize.set(globalQueueService.getQueueSize(QueueType.ORDER));
            cachedGlobalOtherQueueSize.set(globalQueueService.getQueueSize(QueueType.OTHER));
            cachedOrderRetryQueueSize.set(globalQueueService.getRetryQueueSize(QueueType.ORDER));
            cachedOtherRetryQueueSize.set(globalQueueService.getRetryQueueSize(QueueType.OTHER));

            log.debug("Global Queue - ORDER: {}, OTHER: {}",
                    cachedGlobalOrderQueueSize.get(), cachedGlobalOtherQueueSize.get());
        } catch (Exception e) {
            log.warn("Global Queue 크기 업데이트 실패: {}", e.getMessage());
        }
    }

    @Scheduled(fixedDelay = 1000, initialDelay = 1000)
    public void updateGlobalWaterLevel() {
        try {
            long waterLevel = globalRateLimiterService.getCurrentWindowCount();
            cachedGlobalWindowCount.set(waterLevel);

            int capacity = globalRateLimiterService.getCurrentLimit();
            double usage = capacity > 0 ? (double) waterLevel / capacity * 100 : 0;
            log.debug("Global Leaky Bucket: {}/{} ({}%)", waterLevel, capacity, String.format("%.1f", usage));
        } catch (Exception e) {
            log.warn("Global Water Level 업데이트 실패: {}", e.getMessage());
        }
    }

    @Scheduled(fixedDelay = 2000, initialDelay = 1500)
    public void updatePgCurrentTokens() {
        for (PaymentProviderRateLimiter rateLimiter : paymentProviderRateLimiters) {
            String provider = rateLimiter.getProviderName();
            try {
                long available = rateLimiter.getAvailableTokens();
                cachedPgCurrentTokens.get(provider).set(available);

                int max = rateLimiter.getRateLimit();
                long used = max - available;
                double usage = max > 0 ? (double) used / max * 100 : 0;
                log.debug("PG {} 사용량: {}/{} ({}%)", provider, used, max, String.format("%.1f", usage));
            } catch (Exception e) {
                log.warn("PG {} Token 조회 실패: {}", provider, e.getMessage());
            }
        }
    }
}
