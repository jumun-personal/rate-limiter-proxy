package com.jumunhasyeo.ratelimiter.service.pg;

public interface PaymentProviderRateLimiter {

    boolean tryConsume();

    long tryConsumeN(long n);

    void refundN(long n);

    String getProviderName();

    int getRateLimit();

    long getAvailableTokens();
}
