from flow.petri.netbase import Token, NetBase, TransitionAction,\
        TokenColorError, PlaceCapacityError

from flow.protocol.message import Message
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

_SET_TOKEN_SCRIPT = """
local marking_key = KEYS[1]
local global_marking_key = KEYS[2]
local place_key = ARGV[1]
local token_key = ARGV[2]

local set = redis.call('HSETNX', marking_key, place_key, token_key)
if set == 0 then
    local existing_key = redis.call('HGET', marking_key, place_key)
    if existing_key ~= token_key then
        return -1
    else
        return 0
    end
end

redis.call('HINCRBY', global_marking_key, place_key, 1)
return 0
"""

_CONSUME_TOKENS_SCRIPT = """
local state_set_key = KEYS[1]
local active_tokens_key = KEYS[2]
local arcs_in_key = KEYS[3]
local marking_key = KEYS[4]
local global_marking_key = KEYS[5]
local enabler_key = KEYS[6]

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
    token_keys[i] = redis.call('HGET', marking_key, place_id)
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
    redis.call('HDEL', marking_key, arcs_in[i])
    redis.call('HINCRBY', global_marking_key, arcs_in[i], -1)
end
return {0, "Transition enabled"}
"""

_PUSH_TOKENS_SCRIPT = """
local active_tokens_key = KEYS[1]
local arcs_in_key = KEYS[2]
local arcs_out_key = KEYS[3]
local marking_key = KEYS[4]
local global_marking_key = KEYS[5]
local token_key = KEYS[6]
local state_set_key = KEYS[7]
local tokens_pushed_key = KEYS[8]

local n_active_tok = redis.call('LLEN', active_tokens_key)
if n_active_tok == 0 then
    return {-1, "No active tokens"}
end

local arcs_out = redis.call('LRANGE', arcs_out_key, 0, -1)

for i, place_id in pairs(arcs_out) do
    local result = redis.call('HSETNX', marking_key, place_id, token_key)
    if result == false then
        return {-1, "Place " .. place_id .. "is full"}
    end
    redis.call('HINCRBY', global_marking_key, place_id, 1)
end

redis.call('DEL', active_tokens_key)
redis.call('DEL', state_set_key)

local arcs_in = redis.call('LRANGE', arcs_in_key, 0, -1)
for i, place_key in pairs(arcs_in) do
    if redis.call('HEXISTS', marking_key, place_key) == 0 then
        redis.call('SADD', state_set_key, place_key)
    end
end

redis.call('SET', tokens_pushed_key, 1)
return {0, arcs_out}
"""



class _Node(rom.Object):
    name = rom.Property(rom.String)
    arcs_out = rom.Property(rom.List)
    arcs_in = rom.Property(rom.List)


class _Place(_Node):
    first_token_timestamp = rom.Property(rom.Timestamp)
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

    def active_tokens_for_color(self, color_idx):
        return rom.List(connection=self.connection,
                key=self.subkey("%d/active_tokens" % color_idx))

    def state(self, color_idx):
        return rom.Set(connection=self.connection,
                key=self.subkey("%d/state" % color_idx))

    def tokens_pushed(self, color_idx):
        return rom.Int(connection=self.connection,
                key=self.subkey("%d/tokens_pushed" % color_idx))

    def enabler(self, color_idx):
        return rom.String(connection=self.connection,
                key=self.subkey("%d/enabler" % color_idx))


class Net(NetBase):
    place_class = _Place
    transition_class = _Transition

    global_marking = rom.Property(rom.Hash)
    num_token_colors = rom.Property(rom.Int)

    _consume_tokens = rom.Script(_CONSUME_TOKENS_SCRIPT)
    _push_tokens = rom.Script(_PUSH_TOKENS_SCRIPT)
    _set_token = rom.Script(_SET_TOKEN_SCRIPT)

    def _set_initial_transition_state(self):
        pass

    def set_num_token_colors(self, value):
        if self.num_token_colors.setnx(value) == 0:
            raise RuntimeError("Attempted to reset max token color index")

        for tidx in xrange(self.num_transitions.value):
            trans = self.transition(tidx)
            place_indices = trans.arcs_in.value
            for cidx in xrange(value+1):
                trans.state(cidx).value = place_indices

    def marking(self, color_idx):
        return rom.Hash(connection=self.connection,
                key=self.subkey("%d/marking" % int(color_idx)))

    def _validate_token_color(self, color_idx):
        try:
            num_token_colors = self.num_token_colors.value
        except KeyError:
            raise TokenColorError(
                    "Attempted to set token in net %s before setting max token "
                    "color." % self.key)

        return color_idx >= 0 and color_idx < num_token_colors

    def set_token(self, place_idx, token):
        LOG.debug('Putting token (%s) into place (%d) on net (%s)',
                token.key, place_idx, self.key)
        try:
            token_color = token.color_idx.value
        except rom.NotInRedisError:
            raise TokenColorError("Token %s has no color set!" % token.key)

        if not self._validate_token_color(token_color):
            raise TokenColorError("Token=%s: invalid token color %r" %
                    (token.key, token_color))

        marking_key = self.marking(token_color).key
        global_marking_key = self.global_marking.key
        rv = self._set_token(keys=[marking_key, global_marking_key],
                args=[place_idx, token.key])

        if rv != 0:
            raise PlaceCapacityError(
                "Failed to add token %s (color %r) to place %s: "
                "a token already exists" %
                (token.key, token_color, place_idx))

    def notify_place(self, place_idx, token_color, service_interfaces):
        if token_color is None:
            raise RuntimeError("net %s, place %d: token_color is none!" %
                    (self.key, place_idx))

        deferreds = []
        if place_idx in self.marking(token_color):
            place = self.place(place_idx)
            LOG.debug("Notify place net=%s, place=%s (#%d), token_color=%d",
                    self.key, place.name.value, place_idx, token_color)

            place.first_token_timestamp.setnx()
            orchestrator = service_interfaces['orchestrator']
            arcs_out = place.arcs_out.value

            for packet in place.entry_observers.value:
                deferreds.append(orchestrator.place_entry_observed(packet))

            for trans_idx in arcs_out:
                deferred = orchestrator.notify_transition(self.key, int(trans_idx),
                        int(place_idx), token_color=token_color)
                deferreds.append(deferred)

        else:
            LOG.warn("Notify place (%d) on net %s for color (%d), "
                    "but place had no tokens of that color",
                    place_idx, self.key, token_color)

        return defer.DeferredList(deferreds)

    def consume_tokens(self, transition, notifying_place_idx, color_idx):
        active_tokens_key = transition.active_tokens_for_color(color_idx).key
        arcs_in_key = transition.arcs_in.key
        state_key = transition.state(color_idx).key
        enabler_key = transition.enabler(color_idx).key
        marking_key = self.marking(color_idx).key
        global_marking_key = self.global_marking.key

        keys = [state_key, active_tokens_key, arcs_in_key, marking_key,
                global_marking_key, enabler_key]
        args = [notifying_place_idx]

        LOG.debug("Consume tokens: KEYS=%r, ARGS=%r", keys, args)
        LOG.debug("Transition state (color=%d): %r", color_idx,
                transition.state(color_idx).value)
        LOG.debug("Net marking (color=%d): %r", color_idx,
                self.marking(color_idx).value)
        status, message = self._consume_tokens(keys=keys, args=args)
        LOG.debug("Consume tokens (%d) status=%r, message=%r", color_idx,
                status, message)

        return status == 0

    def push_tokens(self, transition, token_key, color_idx):
        active_tokens_key = transition.active_tokens_for_color(color_idx).key
        arcs_in_key = transition.arcs_in.key
        arcs_out_key = transition.arcs_out.key
        marking_key = self.marking(color_idx).key
        global_marking_key = self.global_marking.key
        state_key = transition.state(color_idx).key
        tokens_pushed_key = transition.tokens_pushed(color_idx).key

        keys = [active_tokens_key, arcs_in_key, arcs_out_key, marking_key,
                global_marking_key, token_key, state_key, tokens_pushed_key]
        rv = self._push_tokens(keys=keys)
        tokens_pushed, places_to_notify = rv
        LOG.debug("push_tokens return value: %r", rv)
        tokens_pushed, arcs_out = rv
        tokens_pushed = tokens_pushed == 0

        return tokens_pushed, arcs_out

    def notify_transition(self, trans_idx=None, place_idx=None,
            service_interfaces=None, token_color=None):

        if any([x == None for x in trans_idx, place_idx, service_interfaces]):
            raise TypeError(
                    "You must specify trans_idx, place_idx, and "
                    "service_interfaces")

        if token_color is None:
            raise RuntimeError("Net %s, transition %d: token "
                    "color missing" % (self.key, trans_idx))

        LOG.debug("notify net %s, transition #%d color=%d", self.key, trans_idx,
                token_color)

        trans = self.transition(trans_idx)

        if not self.consume_tokens(trans, place_idx, token_color):
            return defer.succeed(None)

        active_tokens_key = trans.active_tokens_for_color(token_color).key
        tokens_pushed_key = trans.tokens_pushed(token_color).key


        action = trans.action
        new_token = None
        deferreds = []
        if action is not None:
            new_token, deferred = action.execute(active_tokens_key, net=self,
                    service_interfaces=service_interfaces)
            deferreds.append(deferred)

        if new_token is None:
            new_token = self.create_token(token_color=token_color)

        tokens_pushed, arcs_out = self.push_tokens(trans, new_token.key,
                token_color)

        LOG.debug("push_tokens (#%d): pushed=%r, arcs_out=%r", trans_idx,
                tokens_pushed, arcs_out)

        if tokens_pushed is True:
            orchestrator = service_interfaces['orchestrator']
            for place_idx in arcs_out:
                LOG.debug("Notify place %r color=%r", place_idx, token_color)
                deferred = orchestrator.notify_place(self.key, int(place_idx),
                        token_color=token_color)
                deferreds.append(deferred)
            self.connection.delete(tokens_pushed_key)
        return defer.DeferredList(deferreds)

    def _place_plot_name(self, place, idx):
        name = str(place.name)
        ntok = int(self.global_marking.get(idx, 0))
        name += " (%d toks)" % ntok
        return name

    def _place_plot_color(self, place, idx):
        ftt = False
        try:
            ftt = place.first_token_timestamp.value
        except rom.NotInRedisError:
            pass

        ntok = int(self.global_marking.get(idx, 0))
        if ntok > 0:
            return "red"
        elif ftt:
            return "green"
        else:
            return "white"


class ColorJoinAction(TransitionAction):
    arrived = rom.Property(rom.Set)

    def execute(self, active_tokens_key, net, service_interfaces):
        tokens = self.tokens(active_tokens_key)
        if len(tokens) != 1:
            raise RuntimeError("ColorJoinAction should not be able to receive "
                    "multiple tokens at once. Got %r", [x.key for x in tokens])

        color = tokens[0].color_idx.value
        added, size = self.arrived.add_return_size(color)
        if added and size == net.num_token_colors.value:
            self.on_complete(active_tokens_key, net, service_interfaces)
            deferred = self.on_complete(active_tokens_key, net, service_interfaces)
            return None, deferred
        else:
            return None, defer.succeed(None)

    def on_complete(self, active_tokens_key, net, service_interfaces):
        return defer.succeed(None)
