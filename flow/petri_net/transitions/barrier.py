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
local marking_counts_key = KEYS[6]
local enabler_key = KEYS[7]

local place_key = ARGV[1]
local cg_first = ARGV[2]
local cg_end = ARGV[3]

local cg_last = cg_end - 1
local expected_count = cg_end - cg_first

local count = redis.call('HGET', group_marking_key, place_key)
local remaining_tokens = expected_count - count
if remaining_tokens > 0 then
    return {remaining_tokens, "Incoming tokens remaining at place: " .. place_key}
end

redis.call('SREM', state_set_key, place_key)
local remaining_places = redis.call('SCARD', state_set_key)
if remaining_places > 0 then
    return {remaining_places, "Waiting for places"}
end

local enabler_value = redis.call('GET', enabler_key)
if enabler_value == false then
    redis.call('SET', enabler_key, place_key)
elseif enabler_value ~= place_key then
    return {-1, "Transition enabled by a different place: " .. enabler_value}
end

local n_active_tok = redis.call('LLEN', active_tokens_key)
if n_active_tok > 0 then
    return {0, "Transition already has tokens"}
end

local arcs_in = redis.call('LRANGE', arcs_in_key, 0, -1)

local token_counts = {}
local token_keys = {}
remaining_places = 0
for i, place_id in pairs(arcs_in) do
    token_counts[i] = redis.call('HGET', group_marking_key, place_id)
    if token_counts[i] != expected_count then
        redis.call('SADD', state_set_key, place_id)
        remaining_places = remaining_places + 1
    end
end

if remaining_places > 0 then
    return {remaining_places, "Waiting for places"}
end

for i, place_id in pairs(arcs_in) do
    for color = cg_first, cg_last do
        local key = string.format("%s:%s", color, place_id)
        local token_key = redis.call('HGET', color_marking_key, key)
        if token_key == false then
            return {-1, string.format(
                "Mismatch between group and color markings at place %s, " ..
                "color %s", place_id, color)  }
        end
        token_keys[token_key] = {color, place_id}
    end
end

for token_key, color_place_table in pairs(token_keys) do
    local color = color_place_table[1]
    local place_id = color_place_table[2]
    local cp_key = string.format("%s:%s", color, place_id)

    redis.call('LPUSH', active_tokens_key, token_key)
    redis.call('HDEL', color_marking_key, cp_key)
    redis.call('HINCRBY', marking_counts_key, place_id, -1)
    redis.call('HINCRBY', group_marking_key, place_id, -1)
end
return {0, "Transition enabled"}
"""


class BarrierTransition(TransitionBase):
    _consume_tokens = rom.Script(_CONSUME_TOKENS_SCRIPT)

    def consume_tokens(self, net, notifying_place_idx, color_group):
        active_tokens_key = self.active_tokens(color_group.idx).key
        arcs_in_key = self.arcs_in.key
        state_key = self.state(color_group.idx).key
        enabler_key = self.enabler(color_group.idx).key
        color_marking_key = net.color_marking.key
        group_marking_key = net.group_marking(color_group.idx).key
        marking_counts_key = net.marking_counts.key

        keys = [state_key, active_tokens_key, arcs_in_key, color_marking_key,
                group_marking_key, marking_counts_key, enabler_key]
        args = [notifying_place_idx, color_group.begin, color_group.end]

        LOG.debug("Consume tokens: KEYS=%r, ARGS=%r", keys, args)
        LOG.debug("Transition state (color=%d): %r", color_idx,
                self.state(color_group.idx).value)
        LOG.debug("Net marking (color=%d): %r", color_group.idx,
                net.color_marking.value)
        status, message = self._consume_tokens(keys=keys, args=args)
        LOG.debug("Consume tokens (%d) status=%r, message=%r", color_group.idx,
                status, message)

        return status == 0


    def notify(self, net, place_idx, color, service_interfaces):
        raise NotImplementedError()

    def fire(self, net, color, service_interfaces):
        raise NotImplementedError()

    def push_tokens(self, net, tokens, color, service_interfaces):
        raise NotImplementedError()

