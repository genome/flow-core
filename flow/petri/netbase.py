from flow.protocol.message import Message

from uuid import uuid4
import base64
import flow.redisom as rom
import logging
import os
import pwd
import pygraphviz
import subprocess


LOG = logging.getLogger(__name__)

class PlaceCapacityError(Exception):
    pass


class TokenColorError(Exception):
    pass


def make_net_key():
    # Remove the two trailing '=' characters to save space
    return base64.b64encode(uuid4().bytes)[:-2]


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

    optional_fields = {
            "token_color": int
    }


class NotifyTransitionMessage(Message):
    required_fields = {
            "net_key": basestring,
            "place_idx": int,
            "transition_idx": int,
    }

    optional_fields = {
            "token_color": int
    }


class Token(rom.Object):
    data_type = rom.Property(rom.String)
    data = rom.Property(rom.Hash, value_encoder=rom.json_enc,
            value_decoder=rom.json_dec)

    color_idx = rom.Property(rom.Int)

    def _on_create(self):
        try:
            self.data_type.value
        except rom.NotInRedisError:
            self.data_type = ""


class NetBase(rom.Object):
    place_class = None
    transition_class = None

    required_constants = ["environment", "user_id", "working_directory"]

    input_places = rom.Property(rom.Hash)
    output_places = rom.Property(rom.Hash)
    related_nets = rom.Property(rom.List)

    num_places = rom.Property(rom.Int)
    num_transitions = rom.Property(rom.Int)
    variables = rom.Property(rom.Hash, value_encoder=rom.json_enc,
            value_decoder=rom.json_dec)
    _constants = rom.Property(rom.Hash, value_encoder=rom.json_enc,
            value_decoder=rom.json_dec)


    def subkey(self, *args):
        return "/".join([self.key] + [str(x) for x in args])

    @classmethod
    def create(cls, connection=None, name=None, place_names=[],
               trans_actions=[], place_arcs_out={},
               trans_arcs_out={}, key=None):

        if key is None:
            key = make_net_key()
        self = rom.create_object(cls, connection=connection, key=key)

        trans_arcs_in = {}
        for p, trans_set in place_arcs_out.iteritems():
            for t in trans_set:
                trans_arcs_in.setdefault(t, set()).add(p)

        place_arcs_in = {}
        for t, place_set in trans_arcs_out.iteritems():
            for p in place_set:
                place_arcs_in.setdefault(p, set()).add(t)

        self.num_places.value = len(place_names)
        self.num_transitions.value = len(trans_actions)

        for i, pname in enumerate(place_names):
            key = self.subkey("place/%d" % i)
            self.place_class.create(connection=self.connection, key=key,
                    name=pname, arcs_out=place_arcs_out.get(i, {}),
                    arcs_in=place_arcs_in.get(i, {}))

        for i, t in enumerate(trans_actions):
            key = self.subkey("trans/%d" % i)
            name = "" if t is None else t.name
            action_key = None if t is None else t.key
            trans = self.transition_class.create(self.connection, key,
                    name=name, arcs_out=trans_arcs_out.get(i, {}),
                    arcs_in=trans_arcs_in.get(i, {}))

            if action_key is not None:
                trans.action_key = action_key

        self._set_initial_transition_state()

        return self

    def _set_initial_transition_state(self):
        raise NotImplementedError()

    def set_constant(self, key, value):
        if self._constants.setnx(key, value) == 0:
            raise TypeError("Tried to overwrite constant %s in net %s" %
                    (key, self.key))

    def capture_environment(self):
        uid = os.getuid()
        gid = os.getgid()
        user_name = pwd.getpwuid(uid).pw_name
        cwd = os.path.realpath(os.path.curdir)

        self.set_constant("environment", os.environ.data)
        self.set_constant("user_id", uid)
        self.set_constant("group_id", gid)
        self.set_constant("user_name", user_name)
        self.set_constant("working_directory", cwd)

    def copy_constants_from(self, other_net):
        other_net._constants.copy(self._constants.key)

    def constant(self, key):
        return self._constants.get(key)

    def set_variable(self, key, value):
        self.variables[key] = value

    def variable(self, key):
        return self.variables.get(key)

    def place_key(self, idx):
        return self.subkey("place/%d" % int(idx))

    def place(self, idx):
        return self.place_class(self.connection, self.place_key(idx))

    def transition_key(self, idx):
        return self.subkey("trans/%d" % int(idx))

    def transition(self, idx):
        return self.transition_class(self.connection, self.transition_key(idx))

    def copy(self, dst_key):
        copied = rom.Object.copy(self, dst_key)
        for i in xrange(self.num_places.value):
            place_key = copied.subkey("place/%d" % i)
            self.place(i).copy(place_key)

        for i in xrange(self.num_transitions.value):
            trans_key = copied.subkey("trans/%d" % i)
            self.transition(i).copy(trans_key)

        return copied

    def notify_transition(self, trans_idx=None, place_idx=None,
            service_interfaces=None, token_color=None):
        raise NotImplementedError()

    def set_token(self, place_idx, token_key='', service_interfaces=None):
        raise NotImplementedError()

    def _trans_plot_color(self, trans):
        if len(trans.active_tokens) > 0:
            return "blue"

        return "black"

    def _place_plot_color(self, place, idx):
        return "white"

    def _place_plot_name(self, place, idx):
        return str(place.name)

    def graph(self):
        graph = pygraphviz.AGraph(directed=True)

        for i in xrange(self.num_transitions):
            t = self.transition(i)
            ident = "t%i" % i
            name = "%s (#%d)" % (str(t.name), i)
            color = self._trans_plot_color(t)
            graph.add_node(ident, label=name, shape="box",
                    style="filled", fillcolor=color, fontcolor="white")

        for i in xrange(self.num_places):
            p = self.place(i)
            ident = "p%i" % i

            color = self._place_plot_color(p, i)
            name = self._place_plot_name(p, i)

            graph.add_node(ident, label=name, style="filled",
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

    def active_color(self, active_tokens_key):
        first_key = self.connection.lrange(active_tokens_key, 0, 0)
        if not first_key:
            return None

        try:
            token = rom.get_object(self.connection, first_key[0])
            return token.color_idx.value
        except rom.NotInRedisError:
            return None

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
