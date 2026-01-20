package com.jumunhasyeo.ratelimiter.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ScaleOutEventRequest {
    private String source;
    private int newInstanceCount;
    private long timestamp;
}
