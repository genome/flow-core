import flow.redisom as rom
from twisted.internet import defer
import logging

LOG = logging.getLogger(__file__)


_PUSH_TOKENS_SCRIPT = """
local active_tokens_key = KEYS[1]
local arcs_out_key = KEYS[2]
local color_marking_key = KEYS[3]
local group_marking_key = KEYS[4]

local num_tokens = ARGV[1]

local token_color_group_idx = function(idx)
    return 2 + (idx-1)*3
end

local token_color_idx = function(idx)
    return 3 + (idx-1)*3
end

local token_key_idx = function(idx)
    return 4 + (idx-1)*3
end

local n_active_tok = redis.call('LLEN', active_tokens_key)
if n_active_tok == 0 then
    return {-1, "No active tokens"}
end

local arcs_out = redis.call('LRANGE', arcs_out_key, 0, -1)

for i, place_id in pairs(arcs_out) do
    for tok_idx = 1, num_tokens do
        local color_group = ARGV[token_color_group_idx(tok_idx)]
        local color = ARGV[token_color_idx(tok_idx)]
        local token_key = ARGV[token_key_idx(tok_idx)]
        local color_key = string.format("%s:%s", color, place_id)
        local group_key = string.format("%s:%s", color_group, place_id)

        local result = redis.call('HSETNX', color_marking_key, color_key, token_key)
        if result == false then
            return {-1, "Place " .. place_id .. "is full"}
        end
        redis.call('HINCRBY', group_marking_key, group_key, 1)
    end
end

redis.call('DEL', active_tokens_key)

return {0, arcs_out}
"""


class TransitionBase(rom.Object):
    arcs_in = rom.Property(rom.List)
    arcs_out = rom.Property(rom.List)

    enablers = rom.Property(rom.Hash)
    action_key = rom.Property(rom.String)

    tokens_pushed = rom.Property(rom.Int)

    _push_tokens_script = rom.Script(_PUSH_TOKENS_SCRIPT)

    def consume_tokens(self, enabler, color_group, color, color_marking_key,
            group_marking_key):
        raise NotImplementedError()

    def fire(self, net, color_group, color, service_interfaces):
        raise NotImplementedError()

    def push_tokens(self, net, tag, tokens):
        keys = [self.active_tokens(tag).key, self.arcs_out.key,
                net.color_marking.key, net.group_marking.key]

        args = [len(tokens)]
        for t in tokens:
            args.extend([t.color_group_idx.value, t.color.value, t.index.value])

        rv = self._push_tokens_script(keys=keys, args=args)
        LOG.debug("rv=%r", rv)
        return rv[0]

    def notify_places(self, net, color, service_interfaces):
        deferreds = []

        orchestrator = service_interfaces['orchestrator']
        for place_idx in self.arcs_out:
            deferred = orchestrator.notify_place(net.key, place_idx, color)
            deferreds.append(deferred)

        return defer.DeferredList(deferreds)

    def state_key(self, tag):
        return self.subkey("state", tag)

    def active_tokens_key(self, tag):
        return self.subkey("active_tokens", tag)

    def active_tokens(self, tag):
        return rom.List(connection=self.connection,
                key=self.active_tokens_key(tag))
