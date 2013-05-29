local color_marking_key = KEYS[1]
local group_marking_key = KEYS[2]

local place_id = ARGV[1]
local token_idx = ARGV[2]
local color = ARGV[3]
local color_group_idx = ARGV[4]

local color_key = string.format("%s:%s", color, place_id)
local group_key = string.format("%s:%s", color_group_idx, place_id)

local set = redis.call('HSETNX', color_marking_key, color_key, token_idx)
if set == 0 then
    local existing_idx = redis.call('HGET', color_marking_key, color_key)
    if existing_idx ~= token_idx then
        return -1
    else
        return 0
    end
end

redis.call('HINCRBY', group_marking_key, group_key, 1)

return 0
