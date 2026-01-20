package com.jumunhasyeo.ratelimiter.controller;

import com.jumunhasyeo.ratelimiter.service.global.GlobalQueueService;
import com.jumunhasyeo.ratelimiter.service.global.GlobalQueueService.QueueType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/api/v1/queue")
@RequiredArgsConstructor
public class QueueController {

    private final GlobalQueueService globalQueueService;

    @GetMapping("/status")
    public ResponseEntity<QueueStatusResponse> getQueueStatus() {
        return ResponseEntity.ok(new QueueStatusResponse(
                globalQueueService.getQueueSize(QueueType.ORDER),
                globalQueueService.getQueueSize(QueueType.OTHER),
                globalQueueService.getRetryQueueSize(QueueType.ORDER),
                globalQueueService.getRetryQueueSize(QueueType.OTHER),
                globalQueueService.getTotalQueueSize(),
                globalQueueService.getTotalRetryQueueSize()
        ));
    }

    @GetMapping("/position/{userId}")
    public ResponseEntity<QueuePositionResponse> getQueuePosition(
            @PathVariable Long userId,
            @RequestParam(defaultValue = "ORDER") String queueType) {

        QueueType type;
        try {
            type = QueueType.valueOf(queueType.toUpperCase());
        } catch (IllegalArgumentException e) {
            type = QueueType.ORDER;
        }

        Long position = globalQueueService.findSequence(userId, type);
        Long queueSize = globalQueueService.getQueueSize(type);

        return ResponseEntity.ok(new QueuePositionResponse(
                userId,
                position,
                queueSize,
                type.name()
        ));
    }

    public record QueueStatusResponse(
            long orderQueueSize,
            long otherQueueSize,
            long orderRetryQueueSize,
            long otherRetryQueueSize,
            long totalQueueSize,
            long totalRetryQueueSize
    ) {}

    public record QueuePositionResponse(
            Long userId,
            Long position,
            Long totalInQueue,
            String queueType
    ) {}
}
