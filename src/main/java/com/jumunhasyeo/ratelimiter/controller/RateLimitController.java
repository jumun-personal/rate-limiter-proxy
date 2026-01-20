package com.jumunhasyeo.ratelimiter.controller;

import com.jumunhasyeo.ratelimiter.domain.HttpRequestData;
import com.jumunhasyeo.ratelimiter.domain.QueueItem;
import com.jumunhasyeo.ratelimiter.dto.RateLimitRequest;
import com.jumunhasyeo.ratelimiter.dto.RateLimitResponse;
import com.jumunhasyeo.ratelimiter.service.global.GlobalQueueService;
import com.jumunhasyeo.ratelimiter.service.global.GlobalQueueService.QueueType;
import com.jumunhasyeo.ratelimiter.service.global.GlobalRateLimiterService;
import com.jumunhasyeo.ratelimiter.service.global.GlobalRateLimiterService.TryConsumeResult;
import com.jumunhasyeo.ratelimiter.service.pg.RateLimiterService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestClient;

import java.net.URI;

@Slf4j
@RestController
@RequestMapping("/api/v1/ratelimit")
@RequiredArgsConstructor
public class RateLimitController {

    private final GlobalRateLimiterService globalRateLimiterService;
    private final GlobalQueueService globalQueueService;
    private final RateLimiterService pgRateLimiterService;
    private final RestClient orderServiceRestClient;

    private static final String DEFAULT_PROVIDER = "TOSS";

    @PostMapping("/check")
    public ResponseEntity<RateLimitResponse> checkRateLimit(@RequestBody RateLimitRequest request) {
        String path = request.getPath();

        // /api/v1/orders 경로만 처리율 제한 적용
        if (path == null || !path.startsWith("/api/v1/orders")) {
            return ResponseEntity.ok(RateLimitResponse.builder()
                    .allowed(true)
                    .queued(false)
                    .currentLimit(globalRateLimiterService.getCurrentLimit())
                    .build());
        }

        // 전역 처리율 제한 확인
        TryConsumeResult result = globalRateLimiterService.tryConsume(true);

        if (result == TryConsumeResult.ALLOWED) {
            // PG 처리율 제한 확인
            String provider = request.getProvider() != null ? request.getProvider() : DEFAULT_PROVIDER;
            if (path.startsWith("/api/v1/orders/bf")) {
                boolean pgAllowed = pgRateLimiterService.tryConsume(provider);
                if (!pgAllowed) {
                    // PG 큐에 추가
                    return addToQueue(request, true);
                }
            }

            log.debug("Rate Limit 통과: {}", path);

            // 백엔드로 요청 전달
            return forwardRequestToBackend(request);
        }

        // 처리율 초과, 대기열에 추가
        log.debug("Rate Limit 초과, Global Queue에 추가: {}", path);
        return addToQueue(request, false);
    }

    private ResponseEntity<RateLimitResponse> forwardRequestToBackend(RateLimitRequest request) {
        HttpRequestData httpRequest = request.getHttpRequest();
        if (httpRequest == null) {
            log.warn("HTTP 요청 데이터 없음 userId={}", request.getUserId());
            return ResponseEntity.ok(RateLimitResponse.builder()
                    .allowed(true)
                    .queued(false)
                    .currentLimit(globalRateLimiterService.getCurrentLimit())
                    .message("Request allowed (no forwarding data)")
                    .build());
        }

        try {
            URI originalUri = URI.create(httpRequest.getUri());

            RestClient.RequestBodySpec requestSpec = orderServiceRestClient
                    .method(HttpMethod.valueOf(httpRequest.getMethod()))
                    .uri(uriBuilder -> uriBuilder
                            .path(originalUri.getPath())
                            .query(originalUri.getQuery())
                            .build());

            requestSpec.contentType(MediaType.APPLICATION_JSON);

            if (httpRequest.getHeaders() != null) {
                httpRequest.getHeaders().forEach(requestSpec::header);
            }
            if (request.getAccessToken() != null) {
                requestSpec.header("Authorization", "Bearer " + request.getAccessToken());
            }

            if (httpRequest.getBody() != null && !httpRequest.getBody().isEmpty()) {
                requestSpec.body(httpRequest.getBody());
            }

            String backendResponse = requestSpec.retrieve().body(String.class);

            log.debug("Backend로 요청 전달 완료 userId={}, path={}", request.getUserId(), request.getPath());

            return ResponseEntity.ok(RateLimitResponse.builder()
                    .allowed(true)
                    .queued(false)
                    .currentLimit(globalRateLimiterService.getCurrentLimit())
                    .backendResponse(backendResponse)
                    .build());

        } catch (Exception e) {
            log.error("Backend 요청 전달 실패 userId={}: {}", request.getUserId(), e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
                    .body(RateLimitResponse.builder()
                            .allowed(true)
                            .queued(false)
                            .currentLimit(globalRateLimiterService.getCurrentLimit())
                            .message("Backend request failed: " + e.getMessage())
                            .build());
        }
    }

    private ResponseEntity<RateLimitResponse> addToQueue(RateLimitRequest request, boolean isPgQueue) {
        QueueType queueType = globalQueueService.resolveQueueType(
                request.getHttpRequest() != null ? request.getHttpRequest().getMethod() : "GET",
                request.getPath()
        );

        QueueItem item = new QueueItem(
                request.getUserId(),
                request.getAccessToken(),
                request.getHttpRequest()
        );

        boolean added = globalQueueService.offer(item, queueType);

        if (!added) {
            log.error("Queue 추가 실패 userId={}", request.getUserId());
            return ResponseEntity.status(503)
                    .body(RateLimitResponse.builder()
                            .allowed(false)
                            .queued(false)
                            .message("Failed to add to queue")
                            .build());
        }

        Long position = globalQueueService.findSequence(request.getUserId(), queueType);

        return ResponseEntity.accepted()
                .body(RateLimitResponse.builder()
                        .allowed(false)
                        .queued(true)
                        .queuePosition(position)
                        .currentLimit(globalRateLimiterService.getCurrentLimit())
                        .queueType(queueType.name())
                        .message("Request queued")
                        .build());
    }

    @GetMapping("/status")
    public ResponseEntity<RateLimitStatusResponse> getStatus() {
        return ResponseEntity.ok(new RateLimitStatusResponse(
                globalRateLimiterService.getCurrentLimit(),
                globalRateLimiterService.getAvailableTokens(),
                globalRateLimiterService.getCurrentWindowCount(),
                globalQueueService.getQueueSize(QueueType.ORDER),
                globalQueueService.getQueueSize(QueueType.OTHER),
                globalQueueService.getRetryQueueSize(QueueType.ORDER),
                globalQueueService.getRetryQueueSize(QueueType.OTHER)
        ));
    }

    public record RateLimitStatusResponse(
            int currentLimit,
            long availableTokens,
            long currentWindowCount,
            long orderQueueSize,
            long otherQueueSize,
            long orderRetryQueueSize,
            long otherRetryQueueSize
    ) {}
}
