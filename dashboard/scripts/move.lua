-- Atomically move one exact member between job lists.
-- KEYS[1] source (list or zset), KEYS[2] destination list, KEYS[3] job_index hash
-- ARGV[1] source kind ("list"|"zset"), ARGV[2] source member, ARGV[3] destination member,
-- ARGV[4] destination kind ("pending"|"archive"), ARGV[5] job id,
-- ARGV[6] archive max entries, ARGV[7] archive TTL seconds
-- Returns 1 when moved, 0 when the source member is gone (nothing written).
local removed
if ARGV[1] == 'zset' then
  removed = redis.call('ZREM', KEYS[1], ARGV[2])
else
  removed = redis.call('LREM', KEYS[1], 1, ARGV[2])
end
if removed == 0 then
  return 0
end
if ARGV[4] == 'pending' then
  redis.call('LPUSH', KEYS[2], ARGV[3])
  redis.call('HSET', KEYS[3], ARGV[5], ARGV[3])
  return 1
end
redis.call('RPUSH', KEYS[2], ARGV[3])
redis.call('LTRIM', KEYS[2], -tonumber(ARGV[6]), -1)
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[7]))
-- Drop the index only if it still points at the member just removed.
if redis.call('HGET', KEYS[3], ARGV[5]) == ARGV[2] then
  redis.call('HDEL', KEYS[3], ARGV[5])
end
return 1
