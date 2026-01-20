package com.jumunhasyeo.ratelimiter.service.pg;

import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class RateLimiterService {

    private final Map<String, PaymentProviderRateLimiter> rateLimiterMap;

    public RateLimiterService(List<PaymentProviderRateLimiter> rateLimiters) {
        this.rateLimiterMap = rateLimiters.stream()
                .collect(Collectors.toMap(
                        PaymentProviderRateLimiter::getProviderName,
                        Function.identity()
                ));
    }

    /**
     * provider 이름으로 RateLimiter를 조회합니다.
     * @param provider PG 제공자 이름 (대소문자 무관)
     * @return 해당 provider의 RateLimiter
     * @throws IllegalArgumentException provider가 존재하지 않는 경우
     */
    private PaymentProviderRateLimiter getRateLimiter(String provider) {
        PaymentProviderRateLimiter rl = rateLimiterMap.get(provider.toUpperCase());
        if (rl == null) {
            throw new IllegalArgumentException("Unknown provider: " + provider);
        }
        return rl;
    }

    public boolean tryConsume(String provider) {
        return getRateLimiter(provider).tryConsume();
    }

    public long tryConsumeN(String provider, long n) {
        return getRateLimiter(provider).tryConsumeN(n);
    }

    public void refundN(String provider, long n) {
        getRateLimiter(provider).refundN(n);
    }

    public String findAvailableProvider() {
        for (PaymentProviderRateLimiter rateLimiter : rateLimiterMap.values()) {
            if (rateLimiter.tryConsume()) {
                return rateLimiter.getProviderName();
            }
        }
        return null;
    }

    public int getRateLimit(String provider) {
        PaymentProviderRateLimiter rateLimiter = rateLimiterMap.get(provider.toUpperCase());
        return rateLimiter != null ? rateLimiter.getRateLimit() : 0;
    }

    public long getAvailableTokens(String provider) {
        PaymentProviderRateLimiter rateLimiter = rateLimiterMap.get(provider.toUpperCase());
        return rateLimiter != null ? rateLimiter.getAvailableTokens() : 0;
    }
}
