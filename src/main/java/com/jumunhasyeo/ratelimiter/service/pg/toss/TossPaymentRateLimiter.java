package com.jumunhasyeo.ratelimiter.service.pg.toss;

import com.jumunhasyeo.ratelimiter.service.pg.PaymentProviderRateLimiter;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

@Slf4j
@Component
@RequiredArgsConstructor
public class TossPaymentRateLimiter implements PaymentProviderRateLimiter {

    private final StringRedisTemplate redisTemplate;

    private static final String BUCKET_KEY = "leaky:pg:toss";
    private static final int RATE_LIMIT = 10;
    private static final int CAPACITY = 10;
    private static final int TTL_SECONDS = 60;

    @Value("classpath:scripts/pg_refund.lua")
    private Resource refundScriptResource;

    @Value("classpath:scripts/pg_try_consume.lua")
    private Resource tryConsumeScriptResource;

    @Value("classpath:scripts/pg_try_consume_n.lua")
    private Resource tryConsumeNScriptResource;

    @Value("classpath:scripts/pg_get_water_level.lua")
    private Resource getWaterLevelScriptResource;

    private RedisScript<Long> tryConsumeScript;
    private RedisScript<Long> getWaterLevelScript;
    private RedisScript<Long> tryConsumeNScript;
    private RedisScript<Long> refundNScript;

    @PostConstruct
    public void init() throws IOException {
        tryConsumeScript = RedisScript.of(
                tryConsumeScriptResource.getContentAsString(StandardCharsets.UTF_8), Long.class);
        getWaterLevelScript = RedisScript.of(
                getWaterLevelScriptResource.getContentAsString(StandardCharsets.UTF_8), Long.class);
        tryConsumeNScript = RedisScript.of(
                tryConsumeNScriptResource.getContentAsString(StandardCharsets.UTF_8), Long.class);
        refundNScript = RedisScript.of(
                refundScriptResource.getContentAsString(StandardCharsets.UTF_8), Long.class);
        log.debug("TossPaymentRateLimiter: Lua script 파일 로드 완료");
    }

    @Override
    public boolean tryConsume() {
        long now = System.currentTimeMillis();

        try {
            Long result = redisTemplate.execute(
                    tryConsumeScript,
                    Collections.singletonList(BUCKET_KEY),
                    String.valueOf(now),
                    String.valueOf(RATE_LIMIT),
                    String.valueOf(CAPACITY),
                    String.valueOf(TTL_SECONDS)
            );
            return result != null && result == 1L;
        } catch (Exception e) {
            log.error("TOSS tryConsume 오류 발생", e);
            return false;
        }
    }

    @Override
    public long tryConsumeN(long n) {
        long now = System.currentTimeMillis();
        if (n <= 0) return 0L;

        try {
            Long result = redisTemplate.execute(
                    tryConsumeNScript,
                    Collections.singletonList(BUCKET_KEY),
                    String.valueOf(now),
                    String.valueOf(RATE_LIMIT),
                    String.valueOf(CAPACITY),
                    String.valueOf(TTL_SECONDS),
                    String.valueOf(n)
            );
            return result != null ? result : 0L;
        } catch (Exception e) {
            log.error("TOSS tryConsumeN 오류 발생", e);
            return 0L;
        }
    }

    @Override
    public void refundN(long n) {
        if (n <= 0) return;
        long now = System.currentTimeMillis();

        try {
            redisTemplate.execute(
                    refundNScript,
                    Collections.singletonList(BUCKET_KEY),
                    String.valueOf(now),
                    String.valueOf(TTL_SECONDS),
                    String.valueOf(n)
            );
        } catch (Exception e) {
            log.error("TOSS refundN 오류 발생", e);
        }
    }

    @Override
    public String getProviderName() {
        return "TOSS";
    }

    @Override
    public int getRateLimit() {
        return RATE_LIMIT;
    }

    @Override
    public long getAvailableTokens() {
        long now = System.currentTimeMillis();

        try {
            Long result = redisTemplate.execute(
                    getWaterLevelScript,
                    Collections.singletonList(BUCKET_KEY),
                    String.valueOf(now),
                    String.valueOf(RATE_LIMIT)
            );
            if (result != null) {
                long waterLevel = result / 1000;
                return Math.max(0, CAPACITY - waterLevel);
            }
            return CAPACITY;
        } catch (Exception e) {
            log.error("TOSS 사용 가능 Token 조회 오류", e);
            return CAPACITY;
        }
    }
}
