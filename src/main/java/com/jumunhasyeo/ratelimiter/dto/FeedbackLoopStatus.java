package com.jumunhasyeo.ratelimiter.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class FeedbackLoopStatus {
    private boolean active;
    private int previousLimit;
    private int currentLimit;
    private int targetLimit;
    private long activeSince;
    private Double p95Latency;
    private Double p99Latency;
}
