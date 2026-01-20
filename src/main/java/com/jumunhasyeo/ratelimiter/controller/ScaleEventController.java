package com.jumunhasyeo.ratelimiter.controller;

import com.jumunhasyeo.ratelimiter.dto.FeedbackLoopStatus;
import com.jumunhasyeo.ratelimiter.dto.ScaleEventResponse;
import com.jumunhasyeo.ratelimiter.dto.ScaleOutEventRequest;
import com.jumunhasyeo.ratelimiter.service.global.FeedbackLoopStateManager;
import com.jumunhasyeo.ratelimiter.service.global.GlobalRateLimiterService;
import com.jumunhasyeo.ratelimiter.service.global.RedisLatencyHistogramService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;

@Slf4j
@RestController
@RequestMapping("/internal/scale")
@RequiredArgsConstructor
public class ScaleEventController {

    private final FeedbackLoopStateManager stateManager;
    private final GlobalRateLimiterService rateLimiterService;
    private final RedisLatencyHistogramService histogramService;

    @PostMapping("/out")
    public ResponseEntity<ScaleEventResponse> handleScaleOut(@RequestBody ScaleOutEventRequest request) {
        log.debug("Scale-Out 이벤트 수신: source={}, newInstanceCount={}",
                request.getSource(), request.getNewInstanceCount());

        if (stateManager.isActive()) {
            var state = stateManager.getCurrentState();
            return ResponseEntity.ok(ScaleEventResponse.builder()
                    .accepted(false)
                    .previousLimit(state.getPreviousLimit())
                    .currentLimit(rateLimiterService.getCurrentLimit())
                    .message("Feedback loop already active since " +
                            Instant.ofEpochMilli(state.getActivatedAt()))
                    .build());
        }

        var state = stateManager.activateOnScaleOut();
        return ResponseEntity.ok(ScaleEventResponse.builder()
                .accepted(true)
                .previousLimit(state.getPreviousLimit())
                .currentLimit(rateLimiterService.getCurrentLimit())
                .message("Feedback loop activated. Floor set at " + state.getPreviousLimit())
                .build());
    }

    @GetMapping("/status")
    public ResponseEntity<FeedbackLoopStatus> getStatus() {
        var state = stateManager.getCurrentState();
        int currentLimit = rateLimiterService.getCurrentLimit();

        if (!state.isActive()) {
            return ResponseEntity.ok(FeedbackLoopStatus.builder()
                    .active(false)
                    .previousLimit(0)
                    .currentLimit(currentLimit)
                    .targetLimit(0)
                    .activeSince(0)
                    .build());
        }

        double p95 = histogramService.getP95Latency();
        double p99 = histogramService.getP99Latency();

        return ResponseEntity.ok(FeedbackLoopStatus.builder()
                .active(true)
                .previousLimit(state.getPreviousLimit())
                .currentLimit(currentLimit)
                .targetLimit(state.getTargetLimit())
                .activeSince(state.getActivatedAt())
                .p95Latency(p95)
                .p99Latency(p99)
                .build());
    }

    @PostMapping("/deactivate")
    public ResponseEntity<ScaleEventResponse> deactivate() {
        if (!stateManager.isActive()) {
            return ResponseEntity.ok(ScaleEventResponse.builder()
                    .accepted(false)
                    .currentLimit(rateLimiterService.getCurrentLimit())
                    .message("Feedback loop is not active")
                    .build());
        }

        int finalLimit = rateLimiterService.getCurrentLimit();
        stateManager.deactivate();

        return ResponseEntity.ok(ScaleEventResponse.builder()
                .accepted(true)
                .currentLimit(finalLimit)
                .message("Feedback loop deactivated. Final limit: " + finalLimit)
                .build());
    }
}
