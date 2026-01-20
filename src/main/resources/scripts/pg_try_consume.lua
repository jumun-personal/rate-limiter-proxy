--[[
  PG 처리율 제한기 - 단일 토큰 소비 스크립트
  용도: PG 처리율 제한기의 단일 토큰 소비 (큐 검사 없음)

  KEYS[1]: key (leaky:pg:{provider})

  ARGV[1]: now (현재 시간 ms)
  ARGV[2]: leakRate (초당 누수량)
  ARGV[3]: capacity (버킷 용량)
  ARGV[4]: ttl (TTL 초)

  반환값: 1(허용), 0(거절)
]]

local key = KEYS[1]
local now = tonumber(ARGV[1])
local leakRate = tonumber(ARGV[2])
local capacity = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

local waterLevel = tonumber(redis.call('HGET', key, 'water_level') or '0')
local lastLeakTime = tonumber(redis.call('HGET', key, 'last_leak_time') or tostring(now))

local elapsedMs = now - lastLeakTime
local elapsedSec = elapsedMs / 1000.0
local leaked = leakRate * elapsedSec

waterLevel = math.max(0, waterLevel - leaked)

if waterLevel + 1 <= capacity then
    waterLevel = waterLevel + 1
    redis.call('HSET', key, 'water_level', tostring(waterLevel))
    redis.call('HSET', key, 'last_leak_time', tostring(now))
    redis.call('EXPIRE', key, ttl)
    return 1
else
    redis.call('HSET', key, 'water_level', tostring(waterLevel))
    redis.call('HSET', key, 'last_leak_time', tostring(now))
    redis.call('EXPIRE', key, ttl)
    return 0
end
