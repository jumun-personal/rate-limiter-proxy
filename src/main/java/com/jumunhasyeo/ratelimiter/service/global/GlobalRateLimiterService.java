package com.jumunhasyeo.ratelimiter.service.global;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Service
@RequiredArgsConstructor
public class GlobalRateLimiterService {

    private final StringRedisTemplate redisTemplate;

    private static final String KEY = "leaky:global:bucket";
    private static final String ORDER_QUEUE_KEY = "queue:global:order";
    private static final String OTHER_QUEUE_KEY = "queue:global:other";
    private static final String ORDER_RETRY_QUEUE_KEY = "queue:global:order:retry";
    private static final String OTHER_RETRY_QUEUE_KEY = "queue:global:other:retry";
    private static final int TTL_SECONDS = 60;
    private static final int MIN_LIMIT = 10;
    private static final int MAX_LIMIT = 100;

    @Value("${ratelimit.global.rate:15}")
    private int initialRate;

    @Value("${ratelimit.global.capacity:15}")
    private int initialCapacity;

    @Value("classpath:scripts/global_refund.lua")
    private Resource refundScriptResource;

    @Value("classpath:scripts/global_try_consume.lua")
    private Resource tryConsumeScriptResource;

    @Value("classpath:scripts/global_try_consume_n.lua")
    private Resource tryConsumeNScriptResource;

    @Value("classpath:scripts/global_get_water_level.lua")
    private Resource getWaterLevelScriptResource;

    private final AtomicInteger leakRate = new AtomicInteger(15);
    private final AtomicInteger capacity = new AtomicInteger(15);

    private RedisScript<Long> tryConsumeNForQueueScript;
    private RedisScript<Long> tryConsumeScript;
    private RedisScript<Long> getWaterLevelScript;
    private RedisScript<Long> refundNForQueueScript;

    @PostConstruct
    public void init() throws IOException {
        // Lua 스크립트 파일 로드
        tryConsumeScript = RedisScript.of(
                tryConsumeScriptResource.getContentAsString(StandardCharsets.UTF_8), Long.class);
        getWaterLevelScript = RedisScript.of(
                getWaterLevelScriptResource.getContentAsString(StandardCharsets.UTF_8), Long.class);
        tryConsumeNForQueueScript = RedisScript.of(
                tryConsumeNScriptResource.getContentAsString(StandardCharsets.UTF_8), Long.class);
        refundNForQueueScript = RedisScript.of(
                refundScriptResource.getContentAsString(StandardCharsets.UTF_8), Long.class);
        log.debug("Lua script 파일 로드 완료");

        // application.yml 설정값 적용
        leakRate.set(initialRate);
        capacity.set(initialCapacity);
        log.debug("GlobalRateLimiterService 초기화 완료 rate={}, capacity={}", initialRate, initialCapacity);

        // Redis 버킷 초기화 (첫 요청 시 누수가 정상 작동하도록)
        initializeBucket();
    }

    private void initializeBucket() {
        try {
            // 버킷이 존재하지 않으면 초기화
            Boolean exists = redisTemplate.hasKey(KEY);
            if (exists == null || !exists) {
                long now = System.currentTimeMillis();
                redisTemplate.opsForHash().put(KEY, "water_level", "0");
                redisTemplate.opsForHash().put(KEY, "last_leak_time", String.valueOf(now));
                redisTemplate.expire(KEY, java.time.Duration.ofSeconds(TTL_SECONDS));
                log.debug("Redis Bucket 초기화 완료 water_level=0, last_leak_time={}", now);
            }
        } catch (Exception e) {
            log.warn("Redis Bucket 초기화 실패, 첫 요청 시 생성됨", e);
        }
    }

    public boolean tryConsume() {
        return tryConsume(true) == TryConsumeResult.ALLOWED;
    }

    public TryConsumeResult tryConsume(boolean isNewRequest) {
        long now = System.currentTimeMillis();

        List<String> keys = Arrays.asList(
                KEY,
                ORDER_QUEUE_KEY,
                OTHER_QUEUE_KEY,
                ORDER_RETRY_QUEUE_KEY,
                OTHER_RETRY_QUEUE_KEY
        );

        try {
            Long result = redisTemplate.execute(
                    tryConsumeScript,
                    keys,
                    String.valueOf(now),
                    String.valueOf(leakRate.get()),
                    String.valueOf(capacity.get()),
                    String.valueOf(TTL_SECONDS),
                    String.valueOf(isNewRequest ? 1 : 0)
            );
            return TryConsumeResult.fromCode(result != null ? result.intValue() : -999);
        } catch (Exception e) {
            log.error("tryConsume 오류 발생", e);
            return TryConsumeResult.ERROR;
        }
    }

    public long tryConsumeNForQueue(long n) {
        long now = System.currentTimeMillis();
        if (n <= 0) return 0L;

        try {
            Long result = redisTemplate.execute(
                    tryConsumeNForQueueScript,
                    Collections.singletonList(KEY),
                    String.valueOf(now),
                    String.valueOf(leakRate.get()),
                    String.valueOf(capacity.get()),
                    String.valueOf(TTL_SECONDS),
                    String.valueOf(n)
            );
            return result != null ? result : 0L;
        } catch (Exception e) {
            log.error("tryConsumeNForQueue 오류 발생", e);
            return 0L;
        }
    }

    public void refundNForQueue(long n) {
        if (n <= 0) return;
        long now = System.currentTimeMillis();

        try {
            redisTemplate.execute(
                    refundNForQueueScript,
                    Collections.singletonList(KEY),
                    String.valueOf(now),
                    String.valueOf(TTL_SECONDS),
                    String.valueOf(n)
            );
        } catch (Exception e) {
            log.error("refundNForQueue 오류 발생", e);
        }
    }

    public long getAvailableTokens() {
        long waterLevel = getCurrentWaterLevel();
        return Math.max(0, capacity.get() - waterLevel);
    }

    public long getCurrentWindowCount() {
        return getCurrentWaterLevel();
    }

    private long getCurrentWaterLevel() {
        long now = System.currentTimeMillis();

        try {
            Long result = redisTemplate.execute(
                    getWaterLevelScript,
                    Collections.singletonList(KEY),
                    String.valueOf(now),
                    String.valueOf(leakRate.get())
            );
            // Lua에서 waterLevel * 1000을 반환하므로 나눔 (반올림 적용)
            return result != null ? Math.round(result / 1000.0) : 0L;
        } catch (Exception e) {
            log.error("getCurrentWaterLevel 오류 발생", e);
            return 0L;
        }
    }

    public void increaseLimit(int amount) {
        int current = leakRate.get();
        int newRate = Math.min(current + amount, MAX_LIMIT);
        leakRate.set(newRate);
        capacity.set(newRate);
        log.debug("Leak Rate 증가: {} -> {}", current, newRate);
    }

    public void decreaseLimit(int amount) {
        int current = leakRate.get();
        int newRate = Math.max(MIN_LIMIT, current - amount);
        leakRate.set(newRate);
        capacity.set(newRate);
        log.warn("Leak Rate 감소: {} -> {}", current, newRate);
    }

    public int getCurrentLimit() {
        return leakRate.get();
    }

    public void setLimitWithFloor(int newLimit, int floor) {
        int safeLimit = Math.max(newLimit, floor);
        safeLimit = Math.min(safeLimit, MAX_LIMIT);
        safeLimit = Math.max(safeLimit, MIN_LIMIT);

        int current = leakRate.get();
        leakRate.set(safeLimit);
        capacity.set(safeLimit);
        log.debug("Limit 설정 (floor 적용): {} -> {} (floor={})", current, safeLimit, floor);
    }

    public void reset() {
        try {
            redisTemplate.delete(KEY);
            // 초기 설정값으로 복원
            leakRate.set(initialRate);
            capacity.set(initialCapacity);
            // 버킷 재초기화
            initializeBucket();
            log.debug("Rate Limiter 초기화 완료 rate={}, capacity={}", initialRate, initialCapacity);
        } catch (Exception e) {
            log.error("Rate Limiter 초기화 오류", e);
        }
    }

    public boolean isTokenSaturated() {
        long level = getCurrentWaterLevel();
        int cap = capacity.get();
        double usage = (double) level / cap;
        return usage >= 0.9;
    }

    @Getter
    public enum TryConsumeResult {
        ALLOWED(1),
        DENIED_CAPACITY(0),
        DENIED_QUEUE(-1),
        ERROR(-999);

        private final int code;

        TryConsumeResult(int code) {
            this.code = code;
        }

        public static TryConsumeResult fromCode(int code) {
            return switch (code) {
                case 1 -> ALLOWED;
                case 0 -> DENIED_CAPACITY;
                case -1 -> DENIED_QUEUE;
                default -> ERROR;
            };
        }
    }
}
