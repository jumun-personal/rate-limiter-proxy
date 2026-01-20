package com.jumunhasyeo.ratelimiter.service.global;

import jakarta.annotation.PostConstruct;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
@RequiredArgsConstructor
public class FeedbackLoopStateManager {

    private final StringRedisTemplate redisTemplate;
    private final GlobalRateLimiterService rateLimiterService;

    private static final String STATE_KEY = "feedback:loop:state";
    private static final String FIELD_ACTIVE = "active";
    private static final String FIELD_PREVIOUS_LIMIT = "previousLimit";
    private static final String FIELD_TARGET_LIMIT = "targetLimit";
    private static final String FIELD_ACTIVATED_AT = "activatedAt";

    private final AtomicBoolean active = new AtomicBoolean(false);
    private final AtomicInteger previousLimit = new AtomicInteger(0);
    private final AtomicInteger targetLimit = new AtomicInteger(0);
    private final AtomicLong activatedAt = new AtomicLong(0);

    public FeedbackLoopState activateOnScaleOut() {
        if (active.get()) {
            log.warn("Feedback Loop 이미 활성화됨 (since {})",
                    Instant.ofEpochMilli(activatedAt.get()));
            return getCurrentState();
        }

        int currentLimit = rateLimiterService.getCurrentLimit();
        long now = System.currentTimeMillis();

        FeedbackLoopState state = new FeedbackLoopState(true, currentLimit, 0, now);

        persistState(state);
        active.set(true);
        previousLimit.set(currentLimit);
        targetLimit.set(0);
        activatedAt.set(now);
        log.debug("Feedback Loop 활성화됨. Previous Limit (floor): {}", currentLimit);

        return state;
    }

    public void setTargetLimit(int target) {
        targetLimit.set(target);
        persistField(FIELD_TARGET_LIMIT, String.valueOf(target));
        log.debug("Target Limit 설정: {}", target);
    }

    public int getTargetLimit() {
        return targetLimit.get();
    }

    public int getPreviousLimit() {
        return previousLimit.get();
    }

    public boolean isActive() {
        return active.get();
    }

    public long getActivatedAt() {
        return activatedAt.get();
    }

    public void deactivate() {
        try {
            redisTemplate.delete(STATE_KEY);
            active.set(false);
            previousLimit.set(0);
            targetLimit.set(0);
            activatedAt.set(0);
            log.debug("Feedback Loop 비활성화됨");
        } catch (Exception e) {
            log.error("Feedback Loop 비활성화 오류", e);
        }
    }

    public FeedbackLoopState getCurrentState() {
        return new FeedbackLoopState(
                active.get(),
                previousLimit.get(),
                targetLimit.get(),
                activatedAt.get()
        );
    }

    @PostConstruct
    public void restoreState() {
        try {
            Map<Object, Object> entries = redisTemplate.opsForHash().entries(STATE_KEY);

            if (entries.isEmpty()) {
                log.debug("Redis에 기존 Feedback Loop 상태 없음");
                return;
            }

            boolean isActive = "true".equals(entries.get(FIELD_ACTIVE));
            int prevLimit = parseIntOrDefault((String) entries.get(FIELD_PREVIOUS_LIMIT), 0);
            int tgtLimit = parseIntOrDefault((String) entries.get(FIELD_TARGET_LIMIT), 0);
            long actAt = parseLongOrDefault((String) entries.get(FIELD_ACTIVATED_AT), 0);

            active.set(isActive);
            previousLimit.set(prevLimit);
            targetLimit.set(tgtLimit);
            activatedAt.set(actAt);

            if (isActive) {
                log.debug("Redis에서 Feedback Loop 상태 복원됨. Active: {}, PreviousLimit: {}, TargetLimit: {}",
                        isActive, prevLimit, tgtLimit);
            }
        } catch (Exception e) {
            log.error("Feedback Loop 상태 복원 실패", e);
        }
    }

    private void persistState(FeedbackLoopState state) {
        try {
            redisTemplate.opsForHash().putAll(STATE_KEY, Map.of(
                    FIELD_ACTIVE, String.valueOf(state.isActive()),
                    FIELD_PREVIOUS_LIMIT, String.valueOf(state.getPreviousLimit()),
                    FIELD_TARGET_LIMIT, String.valueOf(state.getTargetLimit()),
                    FIELD_ACTIVATED_AT, String.valueOf(state.getActivatedAt())
            ));
        } catch (Exception e) {
            log.error("Feedback Loop 상태 저장 오류", e);
        }
    }

    private void persistField(String field, String value) {
        try {
            redisTemplate.opsForHash().put(STATE_KEY, field, value);
        } catch (Exception e) {
            log.error("Feedback Loop 필드 저장 오류: {}", field, e);
        }
    }

    private int parseIntOrDefault(String value, int defaultValue) {
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private long parseLongOrDefault(String value, long defaultValue) {
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class FeedbackLoopState {
        private boolean active;
        private int previousLimit;
        private int targetLimit;
        private long activatedAt;
    }
}
