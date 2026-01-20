package com.jumunhasyeo.ratelimiter.service.pg;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jumunhasyeo.ratelimiter.domain.QueueItem;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class RedisQueueService {

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    private static final String QUEUE_KEY = "order:queue";

    public boolean offer(QueueItem item) {
        try {
            String value = objectMapper.writeValueAsString(item);
            double score = System.currentTimeMillis();
            Boolean added = redisTemplate.opsForZSet().add(QUEUE_KEY, value, score);
            return added != null && added;
        } catch (JsonProcessingException e) {
            log.error("QueueItem 직렬화 실패", e);
            return false;
        }
    }

    public List<QueueItem> poll(int size) {
        if (size <= 0) {
            return Collections.emptyList();
        }

        Set<String> items = redisTemplate.opsForZSet().range(QUEUE_KEY, 0, size - 1);

        if (items == null || items.isEmpty()) {
            return Collections.emptyList();
        }

        List<QueueItem> result = items.stream()
                .map(this::deserialize)
                .collect(Collectors.toList());

        redisTemplate.opsForZSet().remove(QUEUE_KEY, items.toArray());

        return result;
    }

    public Long findSequence(Long userId) {
        Set<String> allItems = redisTemplate.opsForZSet().range(QUEUE_KEY, 0, -1);

        if (allItems == null) {
            return -1L;
        }

        long position = 0;
        for (String item : allItems) {
            QueueItem queueItem = deserialize(item);
            if (queueItem.getUserId().equals(userId)) {
                return position;
            }
            position++;
        }
        return -1L;
    }

    public Long getQueueSize() {
        Long size = redisTemplate.opsForZSet().size(QUEUE_KEY);
        return size != null ? size : 0L;
    }

    private QueueItem deserialize(String json) {
        try {
            return objectMapper.readValue(json, QueueItem.class);
        } catch (JsonProcessingException e) {
            log.error("QueueItem 역직렬화 실패", e);
            throw new RuntimeException(e);
        }
    }
}
