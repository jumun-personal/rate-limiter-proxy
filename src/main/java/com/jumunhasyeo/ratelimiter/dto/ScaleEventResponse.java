package com.jumunhasyeo.ratelimiter.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ScaleEventResponse {
    private boolean accepted;
    private int previousLimit;
    private int currentLimit;
    private String message;
}
