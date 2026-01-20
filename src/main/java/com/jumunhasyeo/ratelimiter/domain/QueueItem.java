package com.jumunhasyeo.ratelimiter.domain;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;
import java.util.UUID;

@Data
@NoArgsConstructor
public class QueueItem {
    private String requestId;
    private Long userId;
    private String accessToken;
    private HttpRequestData httpRequest;
    private int retryCount;
    private long originalTimestamp;

    public QueueItem(Long userId, String accessToken, HttpRequestData httpRequest) {
        this.requestId = UUID.randomUUID().toString();
        this.userId = userId;
        this.accessToken = accessToken;
        this.httpRequest = httpRequest;
        this.retryCount = 0;
        this.originalTimestamp = System.currentTimeMillis();
    }

    public void incrementRetryCount() {
        this.retryCount++;
    }

    /**
     * 재시도 가능 여부를 반환합니다.
     * @param maxRetryCount 최대 재시도 횟수 (application.yml의 queue.retry.max-retry-count)
     * @return 재시도 가능하면 true
     */
    public boolean canRetry(int maxRetryCount) {
        return this.retryCount < maxRetryCount;
    }

    public boolean isOlderThan(long thresholdMs) {
        return System.currentTimeMillis() - this.originalTimestamp > thresholdMs;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        QueueItem queueItem = (QueueItem) o;
        return retryCount == queueItem.retryCount
                && originalTimestamp == queueItem.originalTimestamp
                && Objects.equals(requestId, queueItem.requestId)
                && Objects.equals(userId, queueItem.userId)
                && Objects.equals(accessToken, queueItem.accessToken)
                && Objects.equals(httpRequest, queueItem.httpRequest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, userId, accessToken, httpRequest, retryCount, originalTimestamp);
    }
}
