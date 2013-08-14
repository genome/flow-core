local active_tokens_key = KEYS[1]
local arcs_out_key = KEYS[2]
local color_marking_key = KEYS[3]
local group_marking_key = KEYS[4]
local transient_keys_key = KEYS[5]

local num_tokens = ARGV[1]

local get_token_color_group = function(idx)
    return ARGV[2 + (idx-1)*3]
end

local get_token_color = function(idx)
    return ARGV[3 + (idx-1)*3]
end

local get_token_key = function(idx)
    return ARGV[4 + (idx-1)*3]
end

local n_active_tok = redis.call('SCARD', active_tokens_key)
if n_active_tok == 0 then
    return {-1, "No active tokens"}
end

local arcs_out = redis.call('LRANGE', arcs_out_key, 0, -1)

for i, place_id in pairs(arcs_out) do
    for tok_idx = 1, num_tokens do
        local color_group = get_token_color_group(tok_idx)
        local color = get_token_color(tok_idx)
        local token_key = get_token_key(tok_idx)
        local color_key = string.format("%s:%s", color, place_id)
        local group_key = string.format("%s:%s", color_group, place_id)

        local result = redis.call('HSETNX', color_marking_key, color_key, token_key)
        if result == 0 then
            return {-1, "Place " .. place_id .. "is full"}
        end
        redis.call('HINCRBY', group_marking_key, group_key, 1)
    end
end

redis.call('DEL', active_tokens_key)
redis.call('SREM', transient_keys_key, active_tokens_key)

return {0, arcs_out}
