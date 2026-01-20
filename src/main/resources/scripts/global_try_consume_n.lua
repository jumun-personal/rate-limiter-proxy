--[[
  전역 처리율 제한기 - N개 토큰 일괄 소비 스크립트
  용도: 전역 처리율 제한기의 N개 토큰 일괄 소비 (큐 처리용)

  KEYS[1]: key (leaky:global:bucket)

  ARGV[1]: now (현재 시간 ms)
  ARGV[2]: leakRate (초당 누수량)
  ARGV[3]: capacity (버킷 용량)
  ARGV[4]: ttl (TTL 초)
  ARGV[5]: requested (요청 토큰 수)

  반환값: 실제 소비된 토큰 수
]]

local key = KEYS[1]
local now = tonumber(ARGV[1])
local leakRate = tonumber(ARGV[2])
local capacity = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
local requested = tonumber(ARGV[5])

local waterLevel = tonumber(redis.call('HGET', key, 'water_level') or '0')
local lastLeakTime = tonumber(redis.call('HGET', key, 'last_leak_time') or tostring(now))

local elapsedMs = now - lastLeakTime
if elapsedMs < 0 then elapsedMs = 0 end
local elapsedSec = elapsedMs / 1000.0
local leaked = leakRate * elapsedSec
waterLevel = math.max(0, waterLevel - leaked)

local available = math.floor(capacity - waterLevel)
if available < 0 then available = 0 end

local consume = math.min(requested, available)
waterLevel = waterLevel + consume

redis.call('HSET', key, 'water_level', tostring(waterLevel))
redis.call('HSET', key, 'last_leak_time', tostring(now))
redis.call('EXPIRE', key, ttl)

return consume
