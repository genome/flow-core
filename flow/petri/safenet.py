from flow.petri.netbase import NetBase, Token, PlaceCapacityError

from flow.protocol.message import Message
from flow.util import stats
from twisted.internet import defer

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

_ADD_TOKEN_SCRIPT = """
local marking_hash = KEYS[1]
local place_key = ARGV[1]
local token_key = ARGV[2]

local set = redis.call('HSETNX', marking_hash, place_key, token_key)
if set == 0 then
    local existing_key = redis.call('HGET', marking_hash, place_key)
    if existing_key ~= token_key then
        return -1
    else
        return 0
    end
end
return 0
"""

_CONSUME_TOKENS_SCRIPT = """
local state_set_key = KEYS[1]
local active_tokens_key = KEYS[2]
local arcs_in_key = KEYS[3]
local marking_hash = KEYS[4]
local enabler_key = KEYS[5]

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
    return {0, "Transition already has tokens"}
end

local arcs_in = redis.call('LRANGE', arcs_in_key, 0, -1)

local token_keys = {}
for i, place_id in pairs(arcs_in) do
    token_keys[i] = redis.call('HGET', marking_hash, place_id)
    if token_keys[i] == false then
        redis.call('SADD', state_set_key, place_id)
    end
end

remaining = redis.call('SCARD', state_set_key)
if remaining > 0 then
    return {remaining, "Incoming tokens remaining"}
end

for i, k in ipairs(token_keys) do
    redis.call('LPUSH', active_tokens_key, k)
    redis.call('HDEL', marking_hash, arcs_in[i])
end
return {0, "Transition enabled"}
"""

_PUSH_TOKENS_SCRIPT = """
local active_tokens_key = KEYS[1]
local arcs_in_key = KEYS[2]
local arcs_out_key = KEYS[3]
local marking_hash_key = KEYS[4]
local token_key = KEYS[5]
local state_set_key = KEYS[6]
local tokens_pushed_key = KEYS[7]

local n_active_tok = redis.call('LLEN', active_tokens_key)
if n_active_tok == 0 then
    return {-1, "No active tokens"}
end

local arcs_out = redis.call('LRANGE', arcs_out_key, 0, -1)

for i, place_id in pairs(arcs_out) do
    local result = redis.call('HSETNX', marking_hash_key, place_id, token_key)
    if result == false then
        return {-1, "Place " .. place_id .. "is full"}
    end
end

redis.call('DEL', active_tokens_key)
redis.call('DEL', state_set_key)

local arcs_in = redis.call('LRANGE', arcs_in_key, 0, -1)
for i, place_key in pairs(arcs_in) do
    if redis.call('HEXISTS', marking_hash_key, place_key) == 0 then
        redis.call('SADD', state_set_key, place_key)
    end
end

redis.call('SET', tokens_pushed_key, 1)
return {1, arcs_out}
"""

class _SafeNode(rom.Object):
    name = rom.Property(rom.String)
    arcs_out = rom.Property(rom.List)
    arcs_in = rom.Property(rom.List)


class _SafePlace(_SafeNode):
    first_token_timestamp = rom.Property(rom.Timestamp)
    entry_observers = rom.Property(rom.List,
            value_encoder=rom.json_enc, value_decoder=rom.json_dec)

    def add_observer(self, exchange, routing_key, body):
        packet = {'exchange': exchange,
                  'routing_key': routing_key,
                  'body': body
                 }
        self.entry_observers.append(packet)


class _SafeTransition(_SafeNode):
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


class SafeNet(NetBase):
    place_class = _SafePlace
    transition_class = _SafeTransition

    marking = rom.Property(rom.Hash)

    _consume_tokens = rom.Script(_CONSUME_TOKENS_SCRIPT)
    _push_tokens = rom.Script(_PUSH_TOKENS_SCRIPT)
    _put_token = rom.Script(_ADD_TOKEN_SCRIPT)

    def _set_initial_transition_state(self):
        for i in xrange(self.num_transitions.value):
            trans = self.transition(i)
            trans.state = trans.arcs_in.value

    @defer.inlineCallbacks
    def notify_transition(self, trans_idx, place_idx,
            service_interfaces, token_color=None):
        if token_color is not None:
            raise RuntimeError("SafeNet %s, transition %d: colored "
                    "tokens are not supported in this net" %
                    (self.key, trans_idx))

        LOG.debug("notify transition #%d", trans_idx)
        timer = stats.create_timer('petri.SafeNet.notify_transition')
        timer.start()

        if trans_idx is None or place_idx is None or service_interfaces is None:
            raise TypeError( "You must specify trans_idx, place_idx, "
                    "and service_interfaces")

        marking_key = self.marking.key

        trans = self.transition(trans_idx)

        active_tokens_key = trans.active_tokens.key
        arcs_in_key = trans.arcs_in.key
        arcs_out_key = trans.arcs_out.key
        state_key = trans.state.key
        tokens_pushed_key = trans.tokens_pushed.key
        enabler_key = trans.enabler.key

        keys = [state_key, active_tokens_key, arcs_in_key, marking_key,
                enabler_key]
        args = [place_idx]
        timer.split('setup')
        rv = self._consume_tokens(keys=keys, args=args)
        timer.split('consume_tokens')
        LOG.debug("Consume tokens rv=%r", rv)

        if rv[0] != 0:
            timer.stop()
            defer.returnValue(None)

        action = trans.action
        new_token = None
        if action is not None:
            new_token = yield action.execute(active_tokens_key, net=self,
                    service_interfaces=service_interfaces)
            timer.split('execute_action.%s' % action.__class__.__name__)

        if new_token is None:
            new_token = self.create_token()
            timer.split('create_token')

        keys = [active_tokens_key, arcs_in_key, arcs_out_key, marking_key,
                new_token.key, state_key, tokens_pushed_key]
        rv = self._push_tokens(keys=keys)
        timer.split('push_tokens')
        tokens_pushed, places_to_notify = rv
        deferreds = []
        if tokens_pushed == 1:
            orchestrator = service_interfaces['orchestrator']
            for place_idx in places_to_notify:
                deferred = orchestrator.notify_place(self.key,
                        int(place_idx), token_color=None)
                deferreds.append(deferred)
            timer.split('set_tokens')
            self.connection.delete(tokens_pushed_key)
            timer.split('delete_pushed_tokens')
        timer.stop()
        yield defer.DeferredList(deferreds)


    def put_token(self, place_idx, token):
        rv = self._put_token(keys=[self.marking.key],
                args=[place_idx, token.key])
        if rv != 0:
            raise PlaceCapacityError(
                "Failed to add token %s to place %s: "
                "a token already exists" %
                (token.key, place_idx))

    def notify_place(self, place_idx, token_color, service_interfaces):
        if token_color is not None:
            raise RuntimeError("Safenet %s place %d got a colored token" %
                    (self.key, place_idx))

        timer = stats.create_timer('petri.SafeNet.notify_place')
        timer.start()

        deferreds = []
        if self.connection.hexists(self.marking.key, place_idx):
            place = self.place(place_idx)
            LOG.debug("Net %s notifying place %s (#%d)", self.key,
                    place.name, place_idx)
            place.first_token_timestamp.setnx()

            orchestrator = service_interfaces['orchestrator']
            arcs_out = place.arcs_out.value

            for packet in place.entry_observers.value:
                deferreds.append(orchestrator.place_entry_observed(packet))

            for trans_idx in arcs_out:
                deferred = orchestrator.notify_transition(self.key,
                        int(trans_idx), int(place_idx))
                deferreds.append(deferred)

        else:
            LOG.warning("Attempted to notify place %d on net (%s), which has no tokens.",
                    place_idx, self.key)

        timer.stop()
        return defer.DeferredList(deferreds)

    def _place_plot_color(self, place, idx):
        ftt = False
        try:
            ftt = place.first_token_timestamp.value
        except rom.NotInRedisError:
            pass

        if idx in self.marking:
            return "red"
        elif ftt:
            return "green"
        else:
            return "white"
