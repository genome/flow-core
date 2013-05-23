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
local color = ARGV[3]

local marking_key = function(color_tag, place_id)
    return string.format("%s:%s", color_tag, place_id)
end

redis.call('SREM', state_set_key, place_key)
local remaining_places = redis.call('SCARD', state_set_key)
if remaining_places > 0 then
    return {remaining_places, "Waiting for places"}
end

local enabler_value = redis.call('HGET', enablers_key, color)
if enabler_value and enabler_value ~= place_key then
    return {-1, "Transition enabled by a different place: " .. enabler_value}
end

local n_active_tok = redis.call('LLEN', active_tokens_key)
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
    return {remaining_places, "Waiting for places (after full check)"}
end

redis.call('HSET', enablers_key, color, place_key)

for place_id, token_key in pairs(token_keys) do
    local cp_key = marking_key(color, place_id)
    local gp_key = marking_key(cg_id, place_id)

    redis.call('LPUSH', active_tokens_key, token_key)
    redis.call('HDEL', color_marking_key, cp_key)
    local res = redis.call('HINCRBY', group_marking_key, gp_key, -1)
    if res == 0 then
        redis.call("HDEL", group_marking_key, gp_key)
    end
end
return {0, "Transition enabled"}
"""


class BasicTransition(TransitionBase):

    _consume_tokens = rom.Script(_CONSUME_TOKENS_SCRIPT)

    def consume_tokens(self, enabler, color_descriptor, color_marking_key,
            group_marking_key):

        active_tokens_key = self.active_tokens_key(color_descriptor)
        state_key = self.state_key(color_descriptor)
        arcs_in_key = self.arcs_in.key
        enablers_key = self.enablers.key

        keys = [state_key, active_tokens_key, arcs_in_key, color_marking_key,
                group_marking_key, enablers_key]
        args = [enabler, color_descriptor.group.idx, color_descriptor.color]

        LOG.debug("Consume tokens: KEYS=%r, ARGS=%r", keys, args)
        rv = self._consume_tokens(keys=keys, args=args)
        LOG.debug("Consume tokens returned: %r", rv)

        return rv[0]

    def state_key(self, color_descriptor):
        return self.subkey("state", color_descriptor.color)

    def active_tokens_key(self, color_descriptor):
        return self.subkey("active_tokens", color_descriptor.color)

    def active_tokens(self, color_descriptor):
        return rom.List(connection=self.connection,
                key=self.active_tokens_key(color_descriptor))
