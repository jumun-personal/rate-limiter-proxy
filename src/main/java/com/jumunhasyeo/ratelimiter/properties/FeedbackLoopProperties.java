package com.jumunhasyeo.ratelimiter.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "feedback-loop")
@Data
public class FeedbackLoopProperties {

    private int intervalMs = 2000;
    private LatencyThresholds latency = new LatencyThresholds();
    private PoolThresholds pool = new PoolThresholds();
    private AdjustmentParams adjustment = new AdjustmentParams();
    private ScaleOutParams scaleOut = new ScaleOutParams();
    private HistogramParams histogram = new HistogramParams();

    @Data
    public static class LatencyThresholds {
        private int p95Good = 500;
        private int p95Bad = 1000;
        private int p99Good = 1000;
        private int p99Bad = 2000;
    }

    @Data
    public static class PoolThresholds {
        private int good = 80;
        private int bad = 95;
    }

    @Data
    public static class AdjustmentParams {
        private double increaseFactor = 0.05;
        private double decreaseFactor = 0.25;
        private int minLimit = 10;
        private int maxLimit = 1000;
        private int maxDecrease = 15;
        private int minIncrease = 2;
    }

    @Data
    public static class ScaleOutParams {
        private int targetDelta = 15;
        private int increaseStepSize = 2;
        private double decreaseRatio = 0.5;
        private int consecutiveHealthyRequired = 3;
        private int consecutiveUnhealthyRequired = 2;
    }

    @Data
    public static class HistogramParams {
        private int timeSliceDurationMs = 10000;
        private int maxSlices = 6;
        private int[] bucketBoundaries = {5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000};
    }
}
