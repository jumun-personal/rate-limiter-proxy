package com.jumunhasyeo.ratelimiter.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "queue.weight")
@Data
public class QueueWeightProperties {

    private int order = 7;
    private int other = 3;
    private double retryRatio = 0.7;

    public int getTotal() {
        return order + other;
    }

    public double getOrderRatio() {
        return (double) order / getTotal();
    }

    public double getOtherRatio() {
        return (double) other / getTotal();
    }

    public double getRetryRatio() {
        return Math.max(0.0, Math.min(1.0, retryRatio));
    }

    public long getRetryDelayMs() {
        return 4000;
    }
}
