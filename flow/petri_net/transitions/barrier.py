import flow.redisom as rom
from base import TransitionBase
import logging

LOG = logging.getLogger(__file__)


_CONSUME_TOKENS_SCRIPT = """
local state_set_key = KEYS[1]
local active_tokens_key = KEYS[2]
local arcs_in_key = KEYS[3]
local color_marking_key = KEYS[4]
local group_marking_key = KEYS[5]
local enablers_key = KEYS[6]

local place_key = ARGV[1]
local cg_id = ARGV[2]
local cg_first = ARGV[3]
local cg_end = ARGV[4]

local marking_key = function(color_tag, place_id)
    return string.format("%s:%s", color_tag, place_id)
end

local cg_last = cg_end - 1
local expected_count = cg_end - cg_first

local count = redis.call('HGET', group_marking_key, marking_key(cg_id, place_key))
if count == false then count = 0 end

local remaining_tokens = expected_count - count
if remaining_tokens > 0 then
    return {remaining_tokens, "Incoming tokens remaining at place: " .. place_key}
end

redis.call('SREM', state_set_key, place_key)
local remaining_places = redis.call('SCARD', state_set_key)
if remaining_places > 0 then
    return {remaining_places, "Waiting for places"}
end

local enabler_value = redis.call('HGET', enablers_key, cg_id)
if enabler_value and enabler_value ~= place_key then
    return {-1, "Transition enabled by a different place: " .. enabler_value}
end

local n_active_tok = redis.call('SCARD', active_tokens_key)
if n_active_tok > 0 then
    return {0, "Transition already has tokens"}
end

local arcs_in = redis.call('LRANGE', arcs_in_key, 0, -1)

local token_counts = {}
remaining_places = 0
for i, place_id in pairs(arcs_in) do
    local key = marking_key(cg_id, place_id)
    token_counts[i] = tonumber(redis.call('HGET', group_marking_key, key))
    if token_counts[i] ~= expected_count then
        redis.call('SADD', state_set_key, place_id)
        remaining_places = remaining_places + 1
    end
end

if remaining_places > 0 then
    return {remaining_places, "Waiting for places (after full check)"}
end

redis.call('HSET', enablers_key, cg_id, place_key)

local token_keys = {}
for i, place_id in pairs(arcs_in) do
    for color = cg_first, cg_last do
        local key = marking_key(color, place_id)
        local token_key = redis.call('HGET', color_marking_key, key)
        if token_key == false then
            return {-1, string.format(
                "Mismatch between group and color markings at place %s, " ..
                "color %s", place_id, color)  }
        end
        table.insert(token_keys, {token_key, color, place_id})
    end
end

for i, token_info in pairs(token_keys) do
    local token_key = token_info[1]
    local color = token_info[2]
    local place_id = token_info[3]
    local cp_key = marking_key(color, place_id)
    local gp_key = marking_key(cg_id, place_id)

    redis.call('SADD', active_tokens_key, token_key)
    redis.call('HDEL', color_marking_key, cp_key)
    local res = redis.call('HINCRBY', group_marking_key, gp_key, -1)
    if res == 0 then
        redis.call("HDEL", group_marking_key, gp_key)
    end
end
return {0, "Transition enabled"}
"""


class BarrierTransition(TransitionBase):
    _consume_tokens = rom.Script(_CONSUME_TOKENS_SCRIPT)

    def consume_tokens(self, enabler, color_descriptor, color_marking_key,
            group_marking_key):

        color_group = color_descriptor.group

        active_tokens_key = self.active_tokens_key(color_descriptor)
        state_key = self.state_key(color_descriptor)
        arcs_in_key = self.arcs_in.key
        enablers_key = self.enablers.key

        keys = [state_key, active_tokens_key, arcs_in_key, color_marking_key,
                group_marking_key, enablers_key]
        args = [enabler, color_group.idx, color_group.begin, color_group.end]

        LOG.debug("Consume tokens: KEYS=%r, ARGS=%r", keys, args)
        rv = self._consume_tokens(keys=keys, args=args)
        LOG.debug("Consume tokens returned: %r", rv)

        return rv[0]

    def state_key(self, color_descriptor):
        return self.subkey("state", color_descriptor.group.idx)

    def active_tokens_key(self, color_descriptor):
        return self.subkey("active_tokens", color_descriptor.group.idx)

    def fire(self, net, color_descriptor, service_interfaces):
        action = self.action
        new_tokens = None
        if action is not None:
            act_toks_key = self.active_tokens_key(color_descriptor)
            new_tokens = action.execute(color_descriptor, act_toks_key, net,
                    service_interfaces)

        return new_tokens
