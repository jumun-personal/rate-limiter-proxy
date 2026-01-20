--[[
  전역 처리율 제한기 - 토큰 환불 스크립트
  용도: 전역 처리율 제한기의 토큰 환불

  KEYS[1]: bucket key (leaky:global:bucket)
  ARGV[1]: now (현재 시간 ms)
  ARGV[2]: ttl (TTL 초)
  ARGV[3]: refund (환불할 토큰 수)

  반환값: 환불 후 수위
]]

local key = KEYS[1]
local now = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
local refund = tonumber(ARGV[3])

local waterLevel = tonumber(redis.call('HGET', key, 'water_level') or '0')
waterLevel = math.max(0, waterLevel - refund)

redis.call('HSET', key, 'water_level', tostring(waterLevel))
redis.call('HSET', key, 'last_leak_time', tostring(now))
redis.call('EXPIRE', key, ttl)

return waterLevel
