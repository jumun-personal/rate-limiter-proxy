--[[
  전역 처리율 제한기 - 단일 토큰 소비 스크립트
  용도: 전역 처리율 제한기의 단일 토큰 소비 (큐 검사 포함)

  KEYS[1]: bucketKey (leaky:global:bucket)
  KEYS[2]: orderQueueKey (queue:global:order)
  KEYS[3]: otherQueueKey (queue:global:other)
  KEYS[4]: orderRetryQueueKey (queue:global:order:retry)
  KEYS[5]: otherRetryQueueKey (queue:global:other:retry)

  ARGV[1]: now (현재 시간 ms)
  ARGV[2]: leakRate (초당 누수량)
  ARGV[3]: capacity (버킷 용량)
  ARGV[4]: ttl (TTL 초)
  ARGV[5]: isNewRequest (1: 신규 요청, 0: 큐 처리)

  반환값: 1(허용), 0(용량 초과), -1(큐 존재)
]]

local bucketKey = KEYS[1]
local orderQueueKey = KEYS[2]
local otherQueueKey = KEYS[3]
local orderRetryQueueKey = KEYS[4]
local otherRetryQueueKey = KEYS[5]

local now = tonumber(ARGV[1])
local leakRate = tonumber(ARGV[2])
local capacity = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
local isNewRequest = tonumber(ARGV[5])

local waterLevel = tonumber(redis.call('HGET', bucketKey, 'water_level') or '0')
local lastLeakTime = tonumber(redis.call('HGET', bucketKey, 'last_leak_time') or tostring(now))

local elapsedMs = now - lastLeakTime
local elapsedSec = elapsedMs / 1000.0
local leaked = leakRate * elapsedSec

waterLevel = math.max(0, waterLevel - leaked)

if isNewRequest == 1 then
    local totalQueueSize = redis.call('ZCARD', orderQueueKey)
                         + redis.call('ZCARD', otherQueueKey)
                         + redis.call('ZCARD', orderRetryQueueKey)
                         + redis.call('ZCARD', otherRetryQueueKey)

    if totalQueueSize > 0 then
        redis.call('HSET', bucketKey, 'water_level', tostring(waterLevel))
        redis.call('HSET', bucketKey, 'last_leak_time', tostring(now))
        redis.call('EXPIRE', bucketKey, ttl)
        return -1
    end
end

if waterLevel + 1 <= capacity then
    waterLevel = waterLevel + 1
    redis.call('HSET', bucketKey, 'water_level', tostring(waterLevel))
    redis.call('HSET', bucketKey, 'last_leak_time', tostring(now))
    redis.call('EXPIRE', bucketKey, ttl)
    return 1
else
    redis.call('HSET', bucketKey, 'water_level', tostring(waterLevel))
    redis.call('HSET', bucketKey, 'last_leak_time', tostring(now))
    redis.call('EXPIRE', bucketKey, ttl)
    return 0
end
