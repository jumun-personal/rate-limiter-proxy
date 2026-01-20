package com.jumunhasyeo.ratelimiter.service.global;

import com.jumunhasyeo.ratelimiter.properties.FeedbackLoopProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
@RequiredArgsConstructor
public class FeedbackLoopScheduler {

    private final PrometheusMetricsCollector prometheusCollector;
    private final RedisLatencyHistogramService histogramService;
    private final GlobalRateLimiterService rateLimiterService;
    private final GlobalQueueService queueService;
    private final FeedbackLoopProperties properties;
    private final FeedbackLoopStateManager stateManager;

    private final AtomicInteger consecutiveHealthyCount = new AtomicInteger(0);
    private final AtomicInteger consecutiveUnhealthyCount = new AtomicInteger(0);

    @Scheduled(fixedDelayString = "${feedback-loop.interval-ms:2000}")
    public void feedbackLoop() {
        if (!stateManager.isActive()) {
            return;
        }

        try {
            MetricsSnapshot metrics = collectMetrics();
            Long queueSize = queueService.getTotalQueueSize();

            log.debug("Feedback Loop - P95: {}ms, P99: {}ms, Queue: {}, Current: {}, Previous(floor): {}, Target: {}",
                    String.format("%.1f", metrics.getP95Latency()),
                    String.format("%.1f", metrics.getP99Latency()),
                    queueSize,
                    rateLimiterService.getCurrentLimit(),
                    stateManager.getPreviousLimit(),
                    stateManager.getTargetLimit());

            evaluateAndAdjustScaleOut(metrics, queueSize);
        } catch (Exception e) {
            log.error("Feedback Loop Metrics 수집 실패", e);
        }
    }

    private MetricsSnapshot collectMetrics() {
        double p95 = histogramService.getP95Latency();
        double p99 = histogramService.getP99Latency();

        PrometheusMetricsCollector.MetricsSnapshot promMetrics = prometheusCollector.collectOrderServiceMetrics();
        double connPool = promMetrics.getConnectionPoolUsage();

        MetricsSnapshot snapshot = new MetricsSnapshot();
        snapshot.setP95Latency(p95);
        snapshot.setP99Latency(p99);
        snapshot.setConnectionPoolUsage(connPool);
        return snapshot;
    }

    private void evaluateAndAdjustScaleOut(MetricsSnapshot metrics, Long queueSize) {
        double p95Ms = metrics.getP95Latency();
        double p99Ms = metrics.getP99Latency();
        double connPool = metrics.getConnectionPoolUsage();

        SystemHealthScore healthScore = calculateHealthScore(p95Ms, p99Ms, connPool);

        log.debug("Health Score: {}/100 ({})",
                String.format("%.1f", healthScore.getScore()),
                healthScore.getLevel());

        int currentLimit = rateLimiterService.getCurrentLimit();
        int previousLimit = stateManager.getPreviousLimit();
        var scaleOutParams = properties.getScaleOut();

        if (healthScore.getScore() < 0) {
            handleUnknown();
            return;
        }

        if (healthScore.getScore() < 50) {
            handleUnhealthyScaleOut(currentLimit, previousLimit, scaleOutParams, healthScore);
            return;
        }

        if (healthScore.getScore() >= 80) {
            handleHealthyScaleOut(currentLimit, previousLimit, queueSize, scaleOutParams);
            return;
        }

        log.debug("보통 상태 ({}) - 현재 유지 {}",
                String.format("%.1f", healthScore.getScore()), currentLimit);
        consecutiveHealthyCount.set(0);
        consecutiveUnhealthyCount.set(0);
    }

    private void handleUnhealthyScaleOut(int currentLimit, int previousLimit,
                                         FeedbackLoopProperties.ScaleOutParams params,
                                         SystemHealthScore healthScore) {
        int unhealthyCount = consecutiveUnhealthyCount.incrementAndGet();
        consecutiveHealthyCount.set(0);

        log.warn("UNHEALTHY - Score: {}/100, 연속 횟수: {}",
                String.format("%.1f", healthScore.getScore()), unhealthyCount);

        if (unhealthyCount >= params.getConsecutiveUnhealthyRequired()) {
            int distanceToFloor = currentLimit - previousLimit;

            if (distanceToFloor <= 0) {
                log.debug("이미 Floor Limit에 도달: {}", previousLimit);
                consecutiveUnhealthyCount.set(0);
                return;
            }

            int decreaseAmount = (int) Math.ceil(distanceToFloor * params.getDecreaseRatio());
            decreaseAmount = Math.max(decreaseAmount, 1);

            int newLimit = Math.max(currentLimit - decreaseAmount, previousLimit);
            int actualDecrease = currentLimit - newLimit;

            if (actualDecrease > 0) {
                rateLimiterService.decreaseLimit(actualDecrease);
                consecutiveUnhealthyCount.set(0);

                log.warn("SCALE-OUT UNHEALTHY: {} -> {} (-{}) [floor={}]",
                        currentLimit, newLimit, actualDecrease, previousLimit);
            }

            stateManager.setTargetLimit(0);
        }
    }

    private void handleHealthyScaleOut(int currentLimit, int previousLimit,
                                       Long queueSize,
                                       FeedbackLoopProperties.ScaleOutParams params) {
        int healthyCount = consecutiveHealthyCount.incrementAndGet();
        consecutiveUnhealthyCount.set(0);

        boolean saturated = rateLimiterService.isTokenSaturated();
        boolean hasDemand = queueSize > 0 || saturated;

        if (!hasDemand) {
            log.debug("정상이지만 수요 없음 - 현재 유지 {}", currentLimit);
            return;
        }

        if (healthyCount >= params.getConsecutiveHealthyRequired()) {
            int targetLimit = stateManager.getTargetLimit();
            int maxLimit = properties.getAdjustment().getMaxLimit();

            if (targetLimit == 0 || currentLimit >= targetLimit) {
                targetLimit = Math.min(currentLimit + params.getTargetDelta(), maxLimit);
                stateManager.setTargetLimit(targetLimit);
                log.debug("새 Target Limit 설정: {} (current: {}, delta: {})",
                        targetLimit, currentLimit, params.getTargetDelta());
            }

            if (currentLimit < targetLimit) {
                int increase = Math.min(
                        params.getIncreaseStepSize(),
                        targetLimit - currentLimit
                );

                rateLimiterService.increaseLimit(increase);
                consecutiveHealthyCount.set(0);

                log.debug("SCALE-OUT HEALTHY: {} -> {} (+{}) [target={}, floor={}]",
                        currentLimit, currentLimit + increase, increase,
                        targetLimit, previousLimit);
            }
        }
    }

    private void handleUnknown() {
        log.debug("UNKNOWN 상태 - Limit 유지: {}",
                rateLimiterService.getCurrentLimit());
        consecutiveHealthyCount.set(0);
        consecutiveUnhealthyCount.set(0);
    }

    private SystemHealthScore calculateHealthScore(double p95Ms, double p99Ms, double connPool) {
        var latency = properties.getLatency();
        var pool = properties.getPool();

        double p95Score = calculateLatencyScore(p95Ms, latency.getP95Good(), latency.getP95Bad());
        double p99Score = calculateLatencyScore(p99Ms, latency.getP99Good(), latency.getP99Bad());
        double poolScore = calculatePoolScore(connPool, pool.getGood(), pool.getBad());

        double totalScore = (p95Score * 0.3) + (p99Score * 0.4) + (poolScore * 0.3);

        if (p95Ms == 0.0 && p99Ms == 0.0 && connPool == 0.0) {
            return new SystemHealthScore(-1, p95Score, p99Score, poolScore);
        }

        return new SystemHealthScore(totalScore, p95Score, p99Score, poolScore);
    }

    private double calculateLatencyScore(double actual, double good, double bad) {
        if (actual <= good) {
            return 100.0;
        } else if (actual >= bad) {
            return 0.0;
        } else {
            return 100.0 * (1 - (actual - good) / (bad - good));
        }
    }

    private double calculatePoolScore(double usage, double good, double bad) {
        if (usage <= good) {
            return 100.0;
        } else if (usage >= bad) {
            return 0.0;
        } else {
            return 100.0 * (1 - (usage - good) / (bad - good));
        }
    }

    @Data
    @AllArgsConstructor
    private static class SystemHealthScore {
        private double score;
        private double p95Score;
        private double p99Score;
        private double poolScore;

        public String getLevel() {
            if (score >= 80) return "EXCELLENT";
            if (score >= 60) return "GOOD";
            if (score >= 30) return "DEGRADED";
            if (score == -1) return "UNKNOWN";
            return "CRITICAL";
        }
    }

    @Data
    private static class MetricsSnapshot {
        private double p95Latency;
        private double p99Latency;
        private double connectionPoolUsage;
    }
}
