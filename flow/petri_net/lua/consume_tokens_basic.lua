local state_set_key = KEYS[1]
local active_tokens_key = KEYS[2]
local arcs_in_key = KEYS[3]
local color_marking_key = KEYS[4]
local group_marking_key = KEYS[5]
local enablers_key = KEYS[6]
local transient_keys_key = KEYS[7]

local place_key = ARGV[1]
local cg_id = ARGV[2]
local color = ARGV[3]

local marking_key = function(color_tag, place_id)
    return string.format("%s:%s", color_tag, place_id)
end

redis.call('SREM', state_set_key, place_key)
local remaining_places = redis.call('SCARD', state_set_key)
if remaining_places > 0 then
    return {remaining_places, "Waiting for places"}
end

redis.call('SREM', transient_keys_key, state_set_key)

local enabler_value = redis.call('HGET', enablers_key, color)
if enabler_value and enabler_value ~= place_key then
    return {-1, "Transition enabled by a different place: " .. enabler_value}
end

local n_active_tok = redis.call('SCARD', active_tokens_key)
if n_active_tok > 0 then
    return {0, "Transition already has tokens"}
end

local arcs_in = redis.call('LRANGE', arcs_in_key, 0, -1)

local token_keys = {}
remaining_places = 0
for i, place_id in pairs(arcs_in) do
    local key = marking_key(color, place_id)
    token_keys[place_id] = redis.call('HGET', color_marking_key, key)
    if token_keys[place_id] == false then
        redis.call('SADD', state_set_key, place_id)
        remaining_places = remaining_places + 1
    end
end

if remaining_places > 0 then
    redis.call('SADD', transient_keys_key, state_set_key)
    return {remaining_places, "Waiting for places (after full check)"}
end

redis.call('HSET', enablers_key, color, place_key)

for place_id, token_key in pairs(token_keys) do
    local cp_key = marking_key(color, place_id)
    local gp_key = marking_key(cg_id, place_id)

    redis.call('SADD', active_tokens_key, token_key)
    redis.call('HDEL', color_marking_key, cp_key)
    local res = redis.call('HINCRBY', group_marking_key, gp_key, -1)
    if res == 0 then
        redis.call("HDEL", group_marking_key, gp_key)
    end
end

redis.call('SADD', transient_keys_key, active_tokens_key)

return {0, "Transition enabled"}
