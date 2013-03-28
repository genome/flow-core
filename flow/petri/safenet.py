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

_COPY_HASH_SCRIPT = """
local src_hash_key = KEYS[1]
local dst_hash_key = KEYS[2]


local data = redis.call('HGETALL', src_hash_key)
if #data == 0 then
    return 0
end

redis.call('DEL', dst_hash_key)
return redis.call('HMSET', dst_hash_key, unpack(data))
"""

_SET_TOKEN_SCRIPT = """
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

def merge_token_data(tokens, data_type=None):
    data = {}
    for token in tokens:
        if not data_type or token.data_type.value == data_type:
            data.update(token.data.value)
    return data


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
    data_type = rom.Property(rom.String)
    data = rom.Property(rom.Hash, value_encoder=rom.json_enc,
            value_decoder=rom.json_dec)

    def _on_create(self):
        try:
            self.data_type.value
        except rom.NotInRedisError:
            self.data_type = ""


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


class SafeNet(rom.Object):
    required_constants = ["environment", "user_id", "working_directory"]

    _copy_hash = rom.Script(_COPY_HASH_SCRIPT)
    _consume_tokens = rom.Script(_CONSUME_TOKENS_SCRIPT)
    _push_tokens = rom.Script(_PUSH_TOKENS_SCRIPT)
    _set_token = rom.Script(_SET_TOKEN_SCRIPT)

    def subkey(self, *args):
        return "/".join([self.key] + [str(x) for x in args])

    @classmethod
    def create(cls, connection=None, name=None, place_names=[],
               trans_actions=[], place_arcs_out={},
               trans_arcs_out={}):

        # Remove the two trailing '=' characters to save space
        key = base64.b64encode(uuid4().bytes)[:-2]
        self = cls(connection, key)
        self._class_info.value = cls._info

        trans_arcs_in = {}
        for p, trans_set in place_arcs_out.iteritems():
            for t in trans_set:
                trans_arcs_in.setdefault(t, set()).add(p)

        self.connection.set(self.subkey("num_places"), len(place_names))
        self.connection.set(self.subkey("num_transitions"), len(trans_actions))

        for i, pname in enumerate(place_names):
            key = self.subkey("place/%d" % i)
            _SafePlace.create(connection=self.connection, key=key, name=pname,
                    arcs_out=place_arcs_out.get(i, {}))

        for i, t in enumerate(trans_actions):
            key = self.subkey("trans/%d" % i)
            name = "" if t is None else t.name
            action_key = None if t is None else t.key
            trans = _SafeTransition.create(self.connection, key, name=name,
                    arcs_out=trans_arcs_out.get(i, {}),
                    arcs_in=trans_arcs_in.get(i, {}),
                    state=trans_arcs_in.get(i, {}))
            if action_key is not None:
                trans.action_key = action_key

        return self

    def set_constant(self, key, value):
        value = json.dumps(value)
        ret = self.connection.hsetnx(self.subkey("constants"), key, value)
        if ret == 0:
            raise TypeError("Attempted to reassign constant property %s" % key)

    def capture_environment(self):
        euid = os.geteuid()
        user_name = pwd.getpwuid(euid).pw_name
        self.set_constant("environment", os.environ.data)
        self.set_constant("user_id", euid)
        self.set_constant("user_name", user_name)
        cwd = os.path.realpath(os.path.curdir)
        self.set_constant("working_directory", cwd)

    def copy_constants_from(self, other_net):
        keys = [other_net.subkey("constants"), self.subkey("constants")]
        rv = self._copy_hash(keys=keys)

    def constant(self, key):
        value = self.connection.hget(self.subkey("constants"), key)
        if value is not None:
            return json.loads(value)

    def set_variable(self, key, value):
        value = json.dumps(value)
        return self.connection.hset(self.subkey("variables"), key, value)

    def variable(self, key):
        value = self.connection.hget(self.subkey("variables"), key)
        if value is not None:
            return json.loads(value)

    @property
    def num_places(self):
        return int(self.connection.get(self.subkey("num_places")) or 0)

    @property
    def num_transitions(self):
        return int(self.connection.get(self.subkey("num_transitions")) or 0)

    def place(self, idx):
        return _SafePlace(self.connection, self.subkey("place/%d" % int(idx)))

    def transition(self, idx):
        return _SafeTransition(self.connection, self.subkey("trans/%d" % int(idx)))

    def notify_transition(self, trans_idx=None, place_idx=None, service_interfaces=None):
        LOG.debug("notify transition #%d", trans_idx)

        if trans_idx is None or place_idx is None or service_interfaces is None:
            raise TypeError(
                    "You must specify trans_idx, place_idx, and service_interfaces")

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
        rv = self._consume_tokens(keys=keys, args=args)
        LOG.debug("Consume tokens rv=%r", rv)

        if rv[0] != 0:
            return

        action = trans.action
        new_token = None
        if action is not None:
            new_token = action.execute(active_tokens_key, net=self,
                    service_interfaces=service_interfaces)

        if new_token is None:
            new_token = Token.create(self.connection)

        keys = [active_tokens_key, arcs_in_key, arcs_out_key, marking_key,
                new_token.key, state_key, tokens_pushed_key]
        rv = self._push_tokens(keys=keys)
        tokens_pushed, places_to_notify = rv
        if tokens_pushed == 1:
            orchestrator = service_interfaces['orchestrator']
            for place_idx in places_to_notify:
                orchestrator.set_token(self.key, int(place_idx), token_key='')
            self.connection.delete(tokens_pushed_key)

    def marking(self, place_idx=None):
        marking_key = self.subkey("marking")

        if place_idx is None:
            return self.connection.hgetall(marking_key)
        else:
            return self.connection.hget(self.subkey("marking"), place_idx)

    def set_token(self, place_idx, token_key='', service_interfaces=None):
        place = self.place(place_idx)
        LOG.debug("Net %s setting token %s for place %s (#%d)", self.key,
                token_key, place.name, place_idx)

        marking_key = self.subkey("marking")

        if token_key:
            rv = self._set_token(keys=[marking_key],
                    args=[place_idx, token_key])
            if rv != 0:
                raise PlaceCapacityError(
                    "Failed to add token %s to place %s: "
                    "a token already exists" %
                    (token_key, place.key))

        if self.connection.hexists(marking_key, place_idx):
            place.first_token_timestamp.setnx()

            orchestrator = service_interfaces['orchestrator']
            arcs_out = place.arcs_out.value

            for packet in place.entry_observers.value:
                orchestrator.place_entry_observed(packet)

            for trans_idx in arcs_out:
                orchestrator.notify_transition(self.key, int(trans_idx),
                        int(place_idx))


    def graph(self):
        graph = pygraphviz.AGraph(directed=True)

        marking = self.marking()

        for i in xrange(self.num_transitions):
            t = self.transition(i)
            ident = "t%i" % i
            name = "%s (#%d)" % (str(t.name), i)
            graph.add_node(ident, label=name, shape="box",
                    style="filled", fillcolor="black", fontcolor="white")

        for i in xrange(self.num_places):
            p = self.place(i)
            ident = "p%i" % i

            ftt = False
            try:
                ftt = p.first_token_timestamp.value
            except rom.NotInRedisError:
                pass

            if str(i) in marking:
                color = "red"
            elif ftt:
                color = "green"
            else:
                color = "white"

            graph.add_node(ident, label=str(p.name), style="filled",
                    fillcolor=color)

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
    required_arguments = []
    output_token_type = ""

    name = rom.Property(rom.String)
    args = rom.Property(rom.Hash, value_encoder=rom.json_enc,
            value_decoder=rom.json_dec)

    def _on_create(self):
        for argname in self.required_arguments:
            if not argname in self.args:
                raise TypeError("In class %s: required argument %s missing" %
                        (self.__class__.__name__, argname))

    def input_data(self, active_tokens_key, net):
        pass

    def tokens(self, active_tokens_key):
        keys = self.connection.lrange(active_tokens_key, 0, -1)
        return [rom.get_object(self.connection, x) for x in keys]

    def execute(self, active_tokens_key, net, service_interfaces):
        raise NotImplementedError("In class %s: execute not implemented" %
                self.__class__.__name__)


class CounterAction(TransitionAction):
    call_count = rom.Property(rom.Int)

    def _on_create(self):
        self.call_count.value = 0

    def execute(self, active_tokens_key, net, service_interfaces):
        self.call_count.incr(1)


class ShellCommandAction(TransitionAction):
    required_arguments = ["command_line", "success_place_id",
            "failure_place_id"]

    def execute(self, active_tokens_key, net, service_interfaces):
        cmdline = self.args["command_line"]
        rv = subprocess.call(cmdline)
        orchestrator = service_interfaces['orchestrator']
        token = Token.create(self.connection, data={"return_code": rv})
        if rv == 0:
            return_place = self.args["success_place_id"]
        else:
            return_place = self.args["failure_place_id"]

        orchestrator.set_token(net.key, int(return_place), token_key=token.key)


class SetRemoteTokenAction(TransitionAction):
    required_arguments = ["remote_net_key", "remote_place_id", "data_type"]

    def execute(self, active_tokens_key, net, service_interfaces):
        remote_net_key = self.args["remote_net_key"]
        remote_place_id = int(self.args["remote_place_id"])
        data_type = self.args["data_type"]

        input_data = self.input_data(active_tokens_key, net)
        token = Token.create(self.connection, data=input_data,
                data_type=data_type)

        orchestrator = service_interfaces['orchestrator']
        orchestrator.set_token(remote_net_key, remote_place_id, token.key)


class MergeTokensAction(TransitionAction):
    required_arguments = ["input_type", "output_type"]

    def execute(self, active_tokens_key, net, service_interfaces):
        token_keys = self.connection.lrange(active_tokens_key, 0, -1)
        tokens = [Token(self.connection, k) for k in token_keys]
        input_type = self.args["input_type"]
        output_type = self.args["output_type"]
        data = merge_token_data(tokens, data_type=input_type)
        return Token.create(self.connection, data_type=output_type, data=data)
