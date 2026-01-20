--[[
  지연 시간 히스토그램 기록 스크립트
  용도: 지연 시간 히스토그램 기록

  KEYS[1]: histogramKey (latency:histogram:{timeSlice})

  ARGV[1]: latencyMs (레이턴시 밀리초)
  ARGV[2]: ttlSeconds (TTL 초)
  ARGV[3]: bucketCount (버킷 개수)
  ARGV[4..]: bucketBoundaries (버킷 경계값들)

  반환값: 1 (성공)
]]

local histogramKey = KEYS[1]
local latencyMs = tonumber(ARGV[1])
local ttlSeconds = tonumber(ARGV[2])
local bucketCount = tonumber(ARGV[3])

for i = 1, bucketCount do
    local boundary = tonumber(ARGV[3 + i])
    if latencyMs <= boundary then
        for j = i, bucketCount do
            local bucketField = ARGV[3 + j]
            redis.call('HINCRBY', histogramKey, bucketField, 1)
        end
        break
    end
end

local highestBucket = ARGV[3 + bucketCount]
local currentCount = tonumber(redis.call('HGET', histogramKey, highestBucket) or '0')
if latencyMs > tonumber(highestBucket) then
    redis.call('HINCRBY', histogramKey, highestBucket, 1)
end

redis.call('EXPIRE', histogramKey, ttlSeconds)
return 1
