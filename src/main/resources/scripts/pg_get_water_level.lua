--[[
  PG 처리율 제한기 - 수위 조회 스크립트
  용도: PG 버킷 수위 조회 (누수 반영)

  KEYS[1]: key (leaky:pg:{provider})

  ARGV[1]: now (현재 시간 ms)
  ARGV[2]: leakRate (초당 누수량)

  반환값: 현재 수위 * 1000 (소수점 정밀도 유지)
]]

local key = KEYS[1]
local now = tonumber(ARGV[1])
local leakRate = tonumber(ARGV[2])

local waterLevel = tonumber(redis.call('HGET', key, 'water_level') or '0')
local lastLeakTime = tonumber(redis.call('HGET', key, 'last_leak_time') or tostring(now))

local elapsedMs = now - lastLeakTime
local elapsedSec = elapsedMs / 1000.0
local leaked = leakRate * elapsedSec

waterLevel = math.max(0, waterLevel - leaked)

return math.floor(waterLevel * 1000)
