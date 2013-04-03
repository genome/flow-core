# FIXME: remove global import
from netbase import *

from flow.protocol.message import Message

from uuid import uuid4
import base64
import flow.redisom as rom
import json
import logging
import os
import pwd
import pygraphviz
import subprocess


LOG = logging.getLogger(__name__)

_CONSUME_TOKENS_SCRIPT = """
local state_set_key = KEYS[1]
local active_tokens_key = KEYS[2]
local enabler_key = KEYS[3]
local FIRST_PLACE_KEY = 4;

local place_key = ARGV[1]

redis.call('SREM', state_set_key, place_key)
local remaining = redis.call('SCARD', state_set_key)
if remaining > 0 then
    return {remaining, "Incoming tokens remaining"}
end

local enabler_value = redis.call('GET', enabler_key)
if enabler_value == false then
    redis.call('SET', enabler_key, place_key)
elseif enabler_value ~= place_key then
    return {-1, "Transition enabled by a different place: " .. enabler_value}
end

local n_active_tok = redis.call('LLEN', active_tokens_key)
if n_active_tok > 0 then
    return {-1, "Transition already has tokens"}
end

local remaining = 0
for i = FIRST_PLACE_KEY, #KEYS, 1 do
    local num_tokens = redis.call('LLEN', KEYS[i])
    if num_tokens == 0 then
        redis.call('SADD', state_set_key, KEYS[i])
        remaining = remaining + 1
    end
end

if remaining > 0 then
    return {remaining, "Incoming tokens remaining"}
end

for i = FIRST_PLACE_KEY, #KEYS, 1 do
    local x = redis.call('LPOP', KEYS[i])
    redis.call('RPUSH', active_tokens_key, x)
end

return {0, "Transition enabled"}
"""

_PUSH_TOKENS_SCRIPT = """
local active_tokens_key = KEYS[1]
local token_key = KEYS[2]
local state_set_key = KEYS[3]
local tokens_pushed_key = KEYS[4]
local FIRST_ARC_IN = 5

local num_arcs_in = ARGV[1]
local FIRST_ARC_OUT = FIRST_ARC_IN + num_arcs_in

local n_active_tok = redis.call('LLEN', active_tokens_key)
if n_active_tok == 0 then
    return {-1, "No active tokens"}
end

for i = FIRST_ARC_OUT, #KEYS, 1 do
    redis.call("RPUSH", KEYS[i], token_key)
end

redis.call('DEL', active_tokens_key)
redis.call('DEL', state_set_key)

for i = FIRST_ARC_IN, FIRST_ARC_OUT-1, 1 do
    if redis.call('LLEN', KEYS[i]) == 0 then
        redis.call('SADD', state_set_key, KEYS[i])
    end
end

redis.call('SET', tokens_pushed_key, 1)
return {0, redis.call('SCARD', state_set_key)}
"""

class _Node(rom.Object):
    name = rom.Property(rom.String)
    arcs_out = rom.Property(rom.List)
    arcs_in = rom.Property(rom.List)


class _Place(_Node):
    first_token_timestamp = rom.Property(rom.Timestamp)
    tokens = rom.Property(rom.List)
    entry_observers = rom.Property(rom.List,
            value_encoder=rom.json_enc, value_decoder=rom.json_dec)

    def add_observer(self, exchange, routing_key, body):
        packet = {'exchange': exchange,
                  'routing_key': routing_key,
                  'body': body
                 }
        self.entry_observers.append(packet)


class _Transition(_Node):
    action_key = rom.Property(rom.String)
    active_tokens = rom.Property(rom.List)
    state = rom.Property(rom.Set)
    tokens_pushed = rom.Property(rom.Int)
    enabler = rom.Property(rom.String)

    @property
    def action(self):
        try:
            key = str(self.action_key)
        except rom.NotInRedisError:
            return None

        return rom.get_object(self.connection, key)


class Net(NetBase):
    place_class = _Place
    transition_class = _Transition

    _marking_strings = rom.Property(rom.Set)

    _consume_tokens = rom.Script(_CONSUME_TOKENS_SCRIPT)
    _push_tokens = rom.Script(_PUSH_TOKENS_SCRIPT)

    def _set_initial_transition_state(self):
        for i in xrange(self.num_transitions.value):
            state = set()
            trans = self.transition(i)
            place_indices = trans.arcs_in.value
            for place_idx in place_indices:
                place = self.place(place_idx)
                state.add(place.tokens.key)
            trans.state = state

    def _marking_string(self, place_idx, token_key):
        return str(place_idx) + token_key

    def consume_tokens(self, transition, notifying_place_idx):
        state_key = transition.state.key
        active_tokens_key = transition.active_tokens.key
        enabler_key = transition.enabler.key

        arcs_in = transition.arcs_in.value
        place_token_keys = [self.subkey("place/%s/tokens" % (x))
                for x in arcs_in]

        keys = [state_key, active_tokens_key, enabler_key] + place_token_keys
        args = [self.subkey("place/%d/tokens" % notifying_place_idx)]
        LOG.debug("KEYS=%r, ARGS=%r", keys, args)
        LOG.debug("Transition state = %r", transition.state.value)
        status, message = self._consume_tokens(keys=keys, args=args)
        LOG.debug("Consume tokens status=%r, message=%r", status, message)

        return status

    def push_tokens(self, transition, token_key):
        active_tokens_key = transition.active_tokens.key

        arcs_in = transition.arcs_in
        arcs_out = transition.arcs_out
        places_in = [self.subkey("place/%s/tokens" % x) for x in arcs_in]
        places_out = [self.subkey("place/%s/tokens" % x) for x in arcs_out]

        keys = [active_tokens_key, token_key, transition.state.key,
                transition.tokens_pushed.key] + places_in + places_out
        args = [len(places_in)]
        LOG.debug("calling push_tokens script with keys=%r, args=%r",
                keys, args)
        rv = self._push_tokens(keys=keys, args=args)
        LOG.debug("push_tokens return value: %r", rv)
        tokens_pushed, tokens_remaining = rv
        tokens_pushed = tokens_pushed == 0

        return tokens_pushed, tokens_remaining

    def notify_transition(self, trans_idx=None, place_idx=None,
            service_interfaces=None):

        LOG.debug("notify net %s, transition #%d", self.key, trans_idx)
        if any([x == None for x in trans_idx, place_idx, service_interfaces]):
            raise TypeError(
                    "You must specify trans_idx, place_idx, and "
                    "service_interfaces")

        trans = self.transition(trans_idx)

        state_key = trans.state.key
        active_tokens_key = trans.active_tokens.key
        tokens_pushed_key = trans.tokens_pushed.key

        status = self.consume_tokens(trans, place_idx)
        if status != 0:
            return

        action = trans.action
        new_token = None
        if action is not None:
            new_token = action.execute(active_tokens_key, net=self,
                    service_interfaces=service_interfaces)

        if new_token is None:
            new_token = Token.create(self.connection)


        tokens_pushed, tokens_remaining = self.push_tokens(trans, new_token.key)

        if tokens_pushed is True:
            orchestrator = service_interfaces['orchestrator']
            arcs_out = trans.arcs_out.value
            for place_idx in arcs_out:
                orchestrator.set_token(self.key, int(place_idx), token_key='')
            self.connection.delete(tokens_pushed_key)


        if tokens_remaining == 0:
            orchestrator.notify_transition(self.key, trans_idx, place_idx)


    def set_token(self, place_idx, token_key='', service_interfaces=None):
        place = self.place(place_idx)
        LOG.debug("Net %s setting token %s for place %s (#%d)", self.key,
                token_key, place.name, place_idx)

        if token_key:
            # FIXME use a set of strings of the form "place_id token_key" to
            # enforce uniqueness of tokens in places
            # we can't use sets on places because SPOP won't work in a script
            place.tokens.append(token_key)

        if len(place.tokens) > 0:
            place.first_token_timestamp.setnx()
            orchestrator = service_interfaces['orchestrator']
            arcs_out = place.arcs_out.value

            for packet in place.entry_observers.value:
                orchestrator.place_entry_observed(packet)

            for trans_idx in arcs_out:
                orchestrator.notify_transition(self.key, int(trans_idx),
                        int(place_idx))

    def _place_plot_name(self, place, idx):
        name = str(place.name)
        ntok = len(place.tokens)
        name += " (%d toks)" % ntok
        return name

    def _place_plot_color(self, place, idx):
        ftt = False
        try:
            ftt = place.first_token_timestamp.value
        except rom.NotInRedisError:
            pass

        if len(place.tokens) > 0:
            return "red"
        elif ftt:
            return "green"
        else:
            return "white"
