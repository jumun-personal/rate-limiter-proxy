--[[
  가중치 기반 큐 폴링 스크립트 (폴링 전용)
  용도: 가중치 기반 큐 폴링 (토큰 버킷 업데이트 없음)

  KEYS[1]: orderNormalKey (queue:global:order)
  KEYS[2]: orderRetryKey (queue:global:order:retry)
  KEYS[3]: otherNormalKey (queue:global:other)
  KEYS[4]: otherRetryKey (queue:global:other:retry)

  ARGV[1]: now (현재 시간 ms)
  ARGV[2]: totalSlots (총 슬롯 수)
  ARGV[3]: orderWeight (ORDER 가중치)
  ARGV[4]: otherWeight (OTHER 가중치)
  ARGV[5]: retryRatio (재시도 비율)
  ARGV[6]: retryThreshold (재시도 임계값)

  반환값: JSON { items: [...], stats: {...} }
]]

local orderNormalKey = KEYS[1]
local orderRetryKey  = KEYS[2]
local otherNormalKey = KEYS[3]
local otherRetryKey  = KEYS[4]

local now            = tonumber(ARGV[1])
local totalSlots     = tonumber(ARGV[2])
local orderWeight    = tonumber(ARGV[3])
local otherWeight    = tonumber(ARGV[4])
local retryRatio     = tonumber(ARGV[5])
local retryThreshold = tonumber(ARGV[6])

if totalSlots <= 0 then
  return cjson.encode({
    items = {},
    stats = { order_retry=0, order_normal=0, other_retry=0, other_normal=0, total_polled=0, remaining_slots=0 }
  })
end

local result = {
  items = {},
  stats = { order_retry=0, order_normal=0, other_retry=0, other_normal=0, total_polled=0, remaining_slots=totalSlots }
}

local function pollFromQueue(key, count, isRetry, threshold)
  if count <= 0 then return {} end

  local items
  if isRetry then
    items = redis.call('ZRANGEBYSCORE', key, '-inf', threshold, 'WITHSCORES', 'LIMIT', 0, count)
  else
    items = redis.call('ZRANGE', key, 0, count - 1, 'WITHSCORES')
  end

  local polled = {}
  local toRemove = {}

  for i = 1, #items, 2 do
    local member = items[i]
    local score = items[i + 1]
    table.insert(polled, { data = member, score = tonumber(score) })
    table.insert(toRemove, member)
  end

  if #toRemove > 0 then
    redis.call('ZREM', key, unpack(toRemove))
  end

  return polled
end

local function calculateSlots(total, orderW, otherW, retryR)
  local totalWeight = orderW + otherW
  local orderTotal = math.floor(total * orderW / totalWeight + 0.5)
  local otherTotal = total - orderTotal

  local orderRetrySlots  = math.floor(orderTotal * retryR + 0.5)
  local orderNormalSlots = orderTotal - orderRetrySlots

  local otherRetrySlots  = math.floor(otherTotal * retryR + 0.5)
  local otherNormalSlots = otherTotal - otherRetrySlots

  return {
    order_retry  = orderRetrySlots,
    order_normal = orderNormalSlots,
    other_retry  = otherRetrySlots,
    other_normal = otherNormalSlots
  }
end

local slots = calculateSlots(totalSlots, orderWeight, otherWeight, retryRatio)
local remainingSlots = totalSlots
local polledItems = {}

-- 1) ORDER retry
local orderRetryItems = pollFromQueue(orderRetryKey, slots.order_retry, true, retryThreshold)
for _, it in ipairs(orderRetryItems) do it.queue='order_retry'; table.insert(polledItems, it) end
result.stats.order_retry = #orderRetryItems
remainingSlots = remainingSlots - #orderRetryItems

-- ORDER retry unused -> ORDER normal
local orderNormalSlots = slots.order_normal + (slots.order_retry - #orderRetryItems)

-- 2) ORDER normal
local orderNormalItems = pollFromQueue(orderNormalKey, orderNormalSlots, false, 0)
for _, it in ipairs(orderNormalItems) do it.queue='order_normal'; table.insert(polledItems, it) end
result.stats.order_normal = #orderNormalItems
remainingSlots = remainingSlots - #orderNormalItems

-- ORDER unused -> OTHER split by retryRatio
local orderTotalUsed = #orderRetryItems + #orderNormalItems
local orderTotalAllocated = slots.order_retry + slots.order_normal
local orderUnused = orderTotalAllocated - orderTotalUsed
if orderUnused < 0 then orderUnused = 0 end

local extraOtherRetry = math.floor(orderUnused * retryRatio + 0.5)
local extraOtherNormal = orderUnused - extraOtherRetry

-- 3) OTHER retry
local otherRetrySlots = slots.other_retry + extraOtherRetry
local otherRetryItems = pollFromQueue(otherRetryKey, otherRetrySlots, true, retryThreshold)
for _, it in ipairs(otherRetryItems) do it.queue='other_retry'; table.insert(polledItems, it) end
result.stats.other_retry = #otherRetryItems
remainingSlots = remainingSlots - #otherRetryItems

-- OTHER retry unused -> OTHER normal (+ extraOtherNormal)
local otherRetryUnused = otherRetrySlots - #otherRetryItems
if otherRetryUnused < 0 then otherRetryUnused = 0 end

local otherNormalSlots = slots.other_normal + otherRetryUnused + extraOtherNormal

-- 4) OTHER normal
local otherNormalItems = pollFromQueue(otherNormalKey, otherNormalSlots, false, 0)
for _, it in ipairs(otherNormalItems) do it.queue='other_normal'; table.insert(polledItems, it) end
result.stats.other_normal = #otherNormalItems
remainingSlots = remainingSlots - #otherNormalItems

result.stats.total_polled = #polledItems
result.stats.remaining_slots = remainingSlots
result.items = polledItems

local itemsJson = "[]"
if #polledItems > 0 then
  local parts = {}
  for i, it in ipairs(polledItems) do
    -- it has fields: queue, data, score
    parts[i] = cjson.encode(it)
  end
  itemsJson = "[" .. table.concat(parts, ",") .. "]"
end

local statsJson = cjson.encode(result.stats)
return '{"items":' .. itemsJson .. ',"stats":' .. statsJson .. '}'
