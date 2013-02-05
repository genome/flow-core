from collections import defaultdict
from flow.protocol.message import Message
from uuid import uuid4
import base64
import flow.orchestrator.redisom as rom
import hashlib
import json
import logging
import os
import pygraphviz
import subprocess
import sys
import time


LOG = logging.getLogger(__name__)

_set_token_script = """
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

_consume_tokens_script = """
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
        return {-2, "Not enough tokens to fire", place_id}
    end
end

for i, k in ipairs(token_keys) do
    redis.call('LPUSH', active_tokens_key, k)
    redis.call('HDEL', marking_hash, arcs_in[i])
end
return {0, "Transition enabled"}
"""

_push_tokens_script = """
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


class SetTokenMessage(Message):
    required_fields = {
            "net_key": basestring,
            "place_idx": int,
            "token_key": basestring,
    }


class NotifyTransitionMessage(Message):
    required_fields = {
            "net_key": basestring,
            "place_idx": int,
            "transition_idx": int,
    }


class PlaceCapacityError(Exception):
    pass


class Token(rom.Object):
    data = rom.Property(rom.Hash, value_encoder=rom.json_enc,
            value_decoder=rom.json_dec)


class _SafeNode(rom.Object):
    name = rom.Property(rom.String)
    arcs_out = rom.Property(rom.List)
    arcs_in = rom.Property(rom.List)


class _SafePlace(_SafeNode):
    first_token_timestamp = rom.Property(rom.Timestamp)
    pass


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


class SafeNet(object):
    _consume_tokens = rom.Script(_consume_tokens_script)
    _push_tokens = rom.Script(_push_tokens_script)
    _set_token = rom.Script(_set_token_script)

    def subkey(self, *args):
        return "/".join([self.key] + [str(x) for x in args])

    @classmethod
    def create(cls, connection=None, name=None, place_names=[],
               trans_actions=[], place_arcs_out={},
               trans_arcs_out={}):

        key = base64.b64encode(uuid4().bytes)
        self = cls(connection, key)

        trans_arcs_in = {}
        for p, trans_set in place_arcs_out.iteritems():
            for t in trans_set:
                trans_arcs_in.setdefault(t, set()).add(p)

        self.conn.set(self.subkey("num_places"), len(place_names))
        self.conn.set(self.subkey("num_transitions"), len(trans_actions))

        for i, pname in enumerate(place_names):
            key = self.subkey("place/%d" % i)
            place = _SafePlace.create(connection=self.conn, key=key, name=pname,
                    arcs_out=place_arcs_out.get(i, {}))

        for i, t in enumerate(trans_actions):
            key = self.subkey("trans/%d" % i)
            name = "" if t is None else t.name
            action_key = None if t is None else t.key
            trans = _SafeTransition.create(self.conn, key, name=name,
                    arcs_out=trans_arcs_out.get(i, {}),
                    arcs_in=trans_arcs_in.get(i, {}),
                    state=trans_arcs_in.get(i, {}))
            if action_key is not None:
                trans.action_key = action_key

        return self

    def set_attribute(self, key, value):
        value = json.dumps(value)
        return self.conn.hset(self.subkey("attributes"), key, value)

    def attribute(self, key):
        value = self.conn.hget(self.subkey("attributes"), key)
        return json.loads(value)

    def __init__(self, conn, key):
        if conn is None:
            raise TypeError("You must supply a valid connection")
        self.conn = conn
        self.key = key

    @property
    def num_places(self):
        np = self.conn.get(self.subkey("num_places")) or 0
        return int(np)

    @property
    def num_transitions(self):
        nt = self.conn.get(self.subkey("num_transitions")) or 0
        return int(nt)

    def place(self, idx):
        return _SafePlace(self.conn, self.subkey("place/%d" % int(idx)))

    def transition(self, idx):
        return _SafeTransition(self.conn, self.subkey("trans/%d" % int(idx)))

    def notify_transition(self, trans_idx=None, place_idx=None, services=None):
        LOG.debug("notify transition #%d", trans_idx)

        if trans_idx is None or place_idx is None or services is None:
            raise TypeError(
                    "You must specify trans_idx, place_idx, and services")

        marking_key = self.subkey("marking")

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
        rv = self._consume_tokens(connection=self.conn, keys=keys, args=args)
        LOG.debug("Consume tokens rv=%r", rv)

        if rv[0] != 0:
            return

        new_token = Token.create(self.conn, data={})
        new_token_key = new_token.key

        action = trans.action
        if action is not None:
            action.execute(self, services)

        keys = [active_tokens_key, arcs_in_key, arcs_out_key, marking_key,
                new_token_key, state_key, tokens_pushed_key]
        rv = self._push_tokens(connection=self.conn, keys=keys)
        tokens_pushed, places_to_notify = rv
        if tokens_pushed == 1:
            orchestrator = services['orchestrator']
            for place_idx in places_to_notify:
                orchestrator.set_token(self.key, int(place_idx), token_key='')
            self.conn.delete(tokens_pushed_key)

    def marking(self, place_idx=None):
        marking_key = self.subkey("marking")

        if place_idx is None:
            return self.conn.hgetall(marking_key)
        else:
            return self.conn.hget(self.subkey("marking"), place_idx)

    def set_token(self, place_idx, token_key='', services=None):
        LOG.debug("setting token for place %s", self.place(place_idx).name)
        place = self.place(place_idx)
        place.first_token_timestamp.setnx()
        marking_key = self.subkey("marking")

        if token_key:
            rv = self._set_token(self.conn, keys=[marking_key],
                    args=[place_idx, token_key])
            if rv != 0:
                raise PlaceCapacityError(
                    "Failed to add token %s to place %s: "
                    "a token already exists" %
                    (token_key, place.key))


        if self.conn.hexists(marking_key, place_idx):
            orchestrator = services['orchestrator']
            arcs_out = place.arcs_out.value
            for trans_idx in arcs_out:
                orchestrator.notify_transition(self.key, int(trans_idx),
                        int(place_idx))

    def graph(self):
        graph = pygraphviz.AGraph(directed=True)

        for i in xrange(self.num_transitions):
            t = self.transition(i)
            ident = "t%i" % i
            graph.add_node(ident, label=str(t.name), shape="box",
                    style="filled", fillcolor="black", fontcolor="white")

        for i in xrange(self.num_places):
            p = self.place(i)
            ident = "p%i" % i
            graph.add_node(ident, label=str(p.name))

        for i in xrange(self.num_transitions):
            t = self.transition(i)
            ident = "t%i" % i
            for dst in t.arcs_out.value:
                graph.add_edge(ident, "p%d" % int(dst))

        for i in xrange(self.num_places):
            p = self.place(i)
            ident = "p%i" % i
            for dst in p.arcs_out.value:
                graph.add_edge(ident, "t%d" % int(dst))

        return graph



class TransitionAction(rom.Object):
    name = rom.Property(rom.String)
    args = rom.Property(rom.List)
    place_refs = rom.Property(rom.List)

    def execute(self, net, services):
        raise NotImplementedError("In class %s: execute not implemented" %
                self.__class__.__name__)


class CounterAction(TransitionAction):
    call_count = rom.Property(rom.Int)

    def _on_create(self):
        self.call_count = 0

    def execute(self, net, services):
        self.call_count.incr(1)


class ShellCommandAction(TransitionAction):
    def execute(self, net, services):
        cmdline = self.args.value
        rv = subprocess.call(cmdline)
        orchestrator = services['orchestrator']
        token = Token.create(self.connection, data={"return_code": rv})
        if rv == 0:
            return_place = self.place_refs[0]
        else:
            return_place = self.place_refs[1]

        orchestrator.set_token(net.key, int(return_place), token_key=token.key)
