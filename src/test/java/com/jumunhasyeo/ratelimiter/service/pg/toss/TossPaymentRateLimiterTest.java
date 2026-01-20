package com.jumunhasyeo.ratelimiter.service.pg.toss;

import com.jumunhasyeo.ratelimiter.support.RedisTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;

import static org.assertj.core.api.Assertions.assertThat;

class TossPaymentRateLimiterTest extends RedisTestBase {

    @Autowired
    private TossPaymentRateLimiter rateLimiter;

    @Autowired
    private StringRedisTemplate redisTemplate;

    private static final String BUCKET_KEY = "leaky:pg:toss";

    @BeforeEach
    void setUp() {
        redisTemplate.delete(BUCKET_KEY);
    }

    @Test
    @DisplayName("용량 내 요청은 허용되어야 한다")
    void tryConsume_shouldAllowWithinCapacity() {
        boolean result = rateLimiter.tryConsume();

        assertThat(result).isTrue();
    }

    @Test
    @DisplayName("용량 초과 시 요청은 거부되어야 한다")
    void tryConsume_shouldDenyWhenCapacityExceeded() {
        int capacity = rateLimiter.getRateLimit(); // CAPACITY = 10

        // 한 번에 모든 용량을 소비 (leak rate 영향 최소화)
        long consumed = rateLimiter.tryConsumeN(capacity);
        assertThat(consumed).isEqualTo(capacity);

        // 즉시 추가 요청 시도 - 거부되어야 함
        boolean result = rateLimiter.tryConsume();

        assertThat(result).isFalse();
    }

    @Test
    @DisplayName("배치 토큰 소비 시 가용량만 반환해야 한다")
    void tryConsumeN_shouldReturnActualConsumed() {
        int capacity = rateLimiter.getRateLimit(); // CAPACITY = 10

        long consumed = rateLimiter.tryConsumeN(capacity + 5);

        // leak rate로 인해 용량보다 약간 더 소비될 수 있음
        // 하지만 요청량을 초과하면 안 됨
        assertThat(consumed).isLessThanOrEqualTo(capacity + 5);
        assertThat(consumed).isGreaterThanOrEqualTo(capacity);
    }

    @Test
    @DisplayName("일부 토큰 소비 후 나머지만 소비 가능해야 한다")
    void tryConsumeN_shouldReturnRemainingAvailable() {
        int capacity = rateLimiter.getRateLimit();
        // 7개 토큰 소비
        rateLimiter.tryConsumeN(7);

        // 5개 더 소비 시도
        long consumed = rateLimiter.tryConsumeN(5);

        // leak rate로 인해 예상 잔량(3)보다 약간 더 소비될 수 있음
        // 3~5 사이여야 함 (경계값 포함)
        assertThat(consumed).isGreaterThanOrEqualTo(3);
        assertThat(consumed).isLessThanOrEqualTo(5);
    }

    @Test
    @DisplayName("토큰 반환 시 가용 토큰이 증가해야 한다")
    void refundN_shouldIncreaseAvailableTokens() {
        int capacity = rateLimiter.getRateLimit();
        // 모든 토큰 소비
        rateLimiter.tryConsumeN(capacity);

        long availableBefore = rateLimiter.getAvailableTokens();

        rateLimiter.refundN(5);

        long availableAfter = rateLimiter.getAvailableTokens();
        // 반환 후 가용 토큰이 증가해야 함
        assertThat(availableAfter).isGreaterThan(availableBefore);
        assertThat(availableAfter - availableBefore).isGreaterThanOrEqualTo(4); // 작은 타이밍 오차 허용
    }

    @Test
    @DisplayName("provider name은 TOSS여야 한다")
    void getProviderName_shouldReturnToss() {
        String providerName = rateLimiter.getProviderName();

        assertThat(providerName).isEqualTo("TOSS");
    }

    @Test
    @DisplayName("rate limit은 10이어야 한다")
    void getRateLimit_shouldReturn10() {
        int rateLimit = rateLimiter.getRateLimit();

        assertThat(rateLimit).isEqualTo(10);
    }

    @Test
    @DisplayName("0 이하의 토큰 요청 시 0을 반환해야 한다")
    void tryConsumeN_shouldReturnZeroForInvalidInput() {
        long consumed = rateLimiter.tryConsumeN(0);
        assertThat(consumed).isEqualTo(0);

        consumed = rateLimiter.tryConsumeN(-5);
        assertThat(consumed).isEqualTo(0);
    }

    @Test
    @DisplayName("초기 상태에서 가용 토큰은 capacity와 같아야 한다")
    void getAvailableTokens_shouldReturnCapacityInitially() {
        long available = rateLimiter.getAvailableTokens();

        assertThat(available).isEqualTo(10);
    }
}
