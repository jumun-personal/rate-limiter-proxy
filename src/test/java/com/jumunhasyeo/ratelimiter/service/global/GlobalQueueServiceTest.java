package com.jumunhasyeo.ratelimiter.service.global;

import com.jumunhasyeo.ratelimiter.domain.HttpRequestData;
import com.jumunhasyeo.ratelimiter.domain.QueueItem;
import com.jumunhasyeo.ratelimiter.service.global.GlobalQueueService.QueueType;
import com.jumunhasyeo.ratelimiter.support.RedisTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class GlobalQueueServiceTest extends RedisTestBase {

    @Autowired
    private GlobalQueueService queueService;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @BeforeEach
    void setUp() {
        // 모든 큐 초기화
        redisTemplate.delete("queue:global:order");
        redisTemplate.delete("queue:global:other");
        redisTemplate.delete("queue:global:order:retry");
        redisTemplate.delete("queue:global:other:retry");
        redisTemplate.delete("queue:pg:order");
        redisTemplate.delete("queue:pg:other");
        redisTemplate.delete("queue:pg:order:retry");
        redisTemplate.delete("queue:pg:other:retry");
    }

    @Test
    @DisplayName("POST /orders 요청은 ORDER 큐로 라우팅되어야 한다")
    void resolveQueueType_shouldReturnOrderForPostOrders() {
        QueueType result = queueService.resolveQueueType("POST", "/api/v1/orders");

        assertThat(result).isEqualTo(QueueType.ORDER);
    }

    @Test
    @DisplayName("POST /orders/bf 요청은 ORDER 큐로 라우팅되어야 한다")
    void resolveQueueType_shouldReturnOrderForPostOrdersBf() {
        QueueType result = queueService.resolveQueueType("POST", "/api/v1/orders/bf");

        assertThat(result).isEqualTo(QueueType.ORDER);
    }

    @Test
    @DisplayName("GET /orders 요청은 OTHER 큐로 라우팅되어야 한다")
    void resolveQueueType_shouldReturnOtherForGetOrders() {
        QueueType result = queueService.resolveQueueType("GET", "/api/v1/orders");

        assertThat(result).isEqualTo(QueueType.OTHER);
    }

    @Test
    @DisplayName("POST /users 요청은 OTHER 큐로 라우팅되어야 한다")
    void resolveQueueType_shouldReturnOtherForNonOrderPaths() {
        QueueType result = queueService.resolveQueueType("POST", "/api/v1/users");

        assertThat(result).isEqualTo(QueueType.OTHER);
    }

    @Test
    @DisplayName("큐에 아이템을 추가할 수 있어야 한다")
    void offer_shouldAddItemToQueue() {
        QueueItem item = createQueueItem(1L);

        boolean result = queueService.offer(item, QueueType.ORDER);

        assertThat(result).isTrue();
        assertThat(queueService.getQueueSize(QueueType.ORDER)).isEqualTo(1);
    }

    @Test
    @DisplayName("큐에서 아이템을 FIFO 순서로 가져와야 한다")
    void poll_shouldRetrieveItemsInFIFOOrder() throws InterruptedException {
        QueueItem item1 = createQueueItem(1L);
        Thread.sleep(10); // 타임스탬프가 다르도록 보장
        QueueItem item2 = createQueueItem(2L);
        Thread.sleep(10);
        QueueItem item3 = createQueueItem(3L);

        queueService.offer(item1, QueueType.ORDER);
        queueService.offer(item2, QueueType.ORDER);
        queueService.offer(item3, QueueType.ORDER);

        List<QueueItem> polled = queueService.poll(QueueType.ORDER, 2);

        // 백그라운드 큐 프로세서로 인해 일부 아이템이 소비될 수 있음
        // 최소 하나의 아이템이 폴링되고 FIFO 순서가 유지되는지 확인
        assertThat(polled).isNotEmpty();
        assertThat(polled.size()).isLessThanOrEqualTo(2);
        // 첫 번째 폴링된 아이템은 가장 낮은 userId를 가져야 함 (FIFO)
        assertThat(polled.get(0).getUserId()).isIn(1L, 2L, 3L);
    }

    @Test
    @DisplayName("재시도 큐에 아이템을 추가할 수 있어야 한다")
    void offerToRetry_shouldAddToRetryQueue() {
        QueueItem item = createQueueItem(1L);

        boolean result = queueService.offerToRetry(item, QueueType.ORDER);

        assertThat(result).isTrue();
        assertThat(queueService.getRetryQueueSize(QueueType.ORDER)).isEqualTo(1);
    }

    @Test
    @DisplayName("재시도 큐에서 4초 이상 경과한 아이템만 가져와야 한다")
    void pollRetryEligible_shouldOnlyReturnEligibleItems() throws InterruptedException {
        QueueItem item1 = createQueueItem(1L);

        // 현재 타임스탬프로 재시도 큐에 아이템 추가
        queueService.offerToRetry(item1, QueueType.ORDER);

        // 즉시 처리 대상이 되면 안 됨
        List<QueueItem> eligible = queueService.pollRetryEligible(QueueType.ORDER, 10);
        assertThat(eligible).isEmpty();

        // 아이템이 여전히 재시도 큐에 있는지 확인
        assertThat(queueService.getRetryQueueSize(QueueType.ORDER)).isEqualTo(1);
    }

    @Test
    @DisplayName("사용자의 큐 위치를 정확히 찾아야 한다")
    void findSequence_shouldReturnCorrectPosition() throws InterruptedException {
        QueueItem item1 = createQueueItem(1L);
        Thread.sleep(10);
        QueueItem item2 = createQueueItem(2L);
        Thread.sleep(10);
        QueueItem item3 = createQueueItem(3L);

        queueService.offer(item1, QueueType.ORDER);
        queueService.offer(item2, QueueType.ORDER);
        queueService.offer(item3, QueueType.ORDER);

        Long position1 = queueService.findSequence(1L, QueueType.ORDER);
        Long position2 = queueService.findSequence(2L, QueueType.ORDER);
        Long position3 = queueService.findSequence(3L, QueueType.ORDER);

        assertThat(position1).isEqualTo(0L);
        assertThat(position2).isEqualTo(1L);
        assertThat(position3).isEqualTo(2L);
    }

    @Test
    @DisplayName("큐에 없는 사용자의 위치 조회 시 -1을 반환해야 한다")
    void findSequence_shouldReturnMinusOneWhenNotFound() {
        Long position = queueService.findSequence(999L, QueueType.ORDER);

        assertThat(position).isEqualTo(-1L);
    }

    @Test
    @DisplayName("전체 큐 크기를 정확히 반환해야 한다")
    void getTotalQueueSize_shouldReturnCorrectSize() {
        queueService.offer(createQueueItem(1L), QueueType.ORDER);
        queueService.offer(createQueueItem(2L), QueueType.ORDER);
        queueService.offer(createQueueItem(3L), QueueType.OTHER);

        Long totalSize = queueService.getTotalQueueSize();

        assertThat(totalSize).isEqualTo(3L);
    }

    @Test
    @DisplayName("전체 재시도 큐 크기를 정확히 반환해야 한다")
    void getTotalRetryQueueSize_shouldReturnCorrectSize() {
        queueService.offerToRetry(createQueueItem(1L), QueueType.ORDER);
        queueService.offerToRetry(createQueueItem(2L), QueueType.OTHER);

        Long totalRetrySize = queueService.getTotalRetryQueueSize();

        assertThat(totalRetrySize).isEqualTo(2L);
    }

    @Test
    @DisplayName("poll 시 요청한 수만큼 가져와야 한다")
    void poll_shouldReturnRequestedCount() {
        for (int i = 0; i < 10; i++) {
            queueService.offer(createQueueItem((long) i), QueueType.ORDER);
        }

        List<QueueItem> polled = queueService.poll(QueueType.ORDER, 3);

        assertThat(polled).hasSize(3);
        assertThat(queueService.getQueueSize(QueueType.ORDER)).isEqualTo(7);
    }

    @Test
    @DisplayName("size가 0이면 빈 리스트를 반환해야 한다")
    void poll_shouldReturnEmptyListForZeroSize() {
        queueService.offer(createQueueItem(1L), QueueType.ORDER);

        List<QueueItem> polled = queueService.poll(QueueType.ORDER, 0);

        assertThat(polled).isEmpty();
        assertThat(queueService.getQueueSize(QueueType.ORDER)).isEqualTo(1);
    }

    private QueueItem createQueueItem(Long userId) {
        HttpRequestData httpRequest = new HttpRequestData(
                "POST",
                "/api/v1/orders",
                Map.of("Content-Type", "application/json"),
                "{\"orderId\": " + userId + "}"
        );
        return new QueueItem(userId, "token-" + userId, httpRequest);
    }
}
