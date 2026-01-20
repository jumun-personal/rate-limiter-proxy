package com.jumunhasyeo.ratelimiter.dto;

import com.jumunhasyeo.ratelimiter.domain.HttpRequestData;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RateLimitRequest {
    private String path;
    private Long userId;
    private String accessToken;
    private HttpRequestData httpRequest;
    private String provider;
}
