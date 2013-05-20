from twisted.internet import defer

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


class Token(rom.Object):
    data_type = rom.Property(rom.String)
    data = rom.Property(rom.Hash, value_encoder=rom.json_enc,
            value_decoder=rom.json_dec)

    net_key = rom.Property(rom.String)

    color_idx = rom.Property(rom.Int)

    @property
    def net(self):
        return rom.get_object(self.connection, self.net_key)

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

    parent_net_key = rom.Property(rom.String)
    child_net_keys = rom.Property(rom.List)

    num_places = rom.Property(rom.Int)
    num_transitions = rom.Property(rom.Int)
    num_tokens = rom.Property(rom.Int)

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

    def capture_environment(self, recursive=True):
        uid = os.getuid()
        gid = os.getgid()
        user_name = pwd.getpwuid(uid).pw_name
        cwd = os.path.realpath(os.path.curdir)

        self.set_constant("environment", os.environ.data)
        self.set_constant("user_id", uid)
        self.set_constant("group_id", gid)
        self.set_constant("user_name", user_name)
        self.set_constant("working_directory", cwd)

        if recursive is True:
            n_children = len(self.child_net_keys)
            children = [self.child_net(x) for x in xrange(n_children)]
            for child in children:
                child.capture_environment(recursive)

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

    @property
    def parent_net(self):
        try:
            key = self.parent_net_key.value
        except rom.NotInRedisError:
            return None

        return rom.get_object(self.connection, key)

    def child_net(self, idx):
        return rom.get_object(self.connection, self.child_net_keys[idx])

    def place(self, idx):
        return self.place_class(self.connection, self.place_key(idx))

    def transition_key(self, idx):
        return self.subkey("trans/%d" % int(idx))

    def transition(self, idx):
        return self.transition_class(self.connection, self.transition_key(idx))


    def token_key(self, idx):
        return self.subkey("tok/%d" % int(idx))

    def token(self, idx):
        return Token(self.connection, self.token_key(idx))

    def _next_token_key(self):
        return self.token_key(self.num_tokens.incr() - 1)

    def create_token(self, data=None, data_type=None, token_color=None):
        key = self._next_token_key()
        return Token.create(self.connection, key, net_key=self.key,
                data=data, data_type=data_type, color_idx=token_color)


    def create_put_notify(self, place_idx, service_interfaces,
            token_color=None, **create_token_kwargs):
        token = self.create_token(
                token_color=token_color, **create_token_kwargs)
        self.put_token(place_idx, token)
        return self.notify_place(place_idx, token_color=token_color,
                service_interfaces=service_interfaces)


    def copy(self, dst_key):
        copied = rom.Object.copy(self, dst_key)
        for i in xrange(self.num_places.value):
            place_key = copied.subkey("place/%d" % i)
            self.place(i).copy(place_key)

        for i in xrange(self.num_transitions.value):
            trans_key = copied.subkey("trans/%d" % i)
            self.transition(i).copy(trans_key)

        return copied

    def delete(self):
        for i in xrange(self.num_places.value):
            place = self.place(i)
            place.delete()

        for i in xrange(self.num_transitions.value):
            transition = self.transition(i)
            transition.delete()

        try:
            for i in xrange(self.num_tokens.value):
                token = self.token(i)
                token.delete()
        except rom.NotInRedisError:  # Wanted default value 0
            pass

        rom.Object.delete(self)

    def put_token(self, place_idx, token):
        raise NotImplementedError()

    def notify_place(self, place_idx, token_color, service_interfaces):
        """
        Returns a deferred that fires when all service_interface related
        deferreds have fired.
        """
        raise NotImplementedError()

    def notify_transition(self, trans_idx, place_idx,
            service_interfaces, token_color):
        """
        Returns a deferred that fires when all service_interface related
        deferreds have fired.
        """
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
        """
        Returns a deferred that will callback (with either a new_token or
        None) once the action has been completed.
        """
        raise NotImplementedError("In class %s: execute not implemented" %
                self.__class__.__name__)


class CounterAction(TransitionAction):
    call_count = rom.Property(rom.Int)

    def _on_create(self):
        self.call_count.value = 0

    def execute(self, active_tokens_key, net, service_interfaces):
        self.call_count.incr(1)
        return defer.succeed(None)


class ShellCommandAction(TransitionAction):
    required_arguments = ["command_line", "success_place_id",
            "failure_place_id"]

    def execute(self, active_tokens_key, net, service_interfaces):
        cmdline = self.args["command_line"]
        rv = subprocess.call(cmdline)
        orchestrator = service_interfaces['orchestrator']
        if rv == 0:
            return_place = self.args["success_place_id"]
        else:
            return_place = self.args["failure_place_id"]

        token_color = self.active_color(active_tokens_key)
        deferred = orchestrator.create_token(net.key, int(return_place),
                token_color=token_color, data={"return_code": rv})

        execute_deferred = defer.Deferred()
        deferred.addCallback(lambda _: execute_deferred.callback(None))
        return execute_deferred


class SetRemoteTokenAction(TransitionAction):
    required_arguments = ["remote_net_key", "remote_place_id", "data_type"]

    def execute(self, active_tokens_key, net, service_interfaces):
        remote_net_key = self.args["remote_net_key"]
        remote_place_id = int(self.args["remote_place_id"])
        data_type = self.args["data_type"]

        input_data = self.input_data(active_tokens_key, net)

        orchestrator = service_interfaces['orchestrator']
        token_color = self.active_color(active_tokens_key)
        deferred = orchestrator.create_token(remote_net_key, remote_place_id,
                token_color=token_color, data=input_data, data_type=data_type)

        execute_deferred = defer.Deferred()
        deferred.addCallback(lambda _: execute_deferred.callback(None))
        return execute_deferred


class MergeTokensAction(TransitionAction):
    required_arguments = ["input_type", "output_type"]

    def execute(self, active_tokens_key, net, service_interfaces):
        token_keys = self.connection.lrange(active_tokens_key, 0, -1)
        tokens = [Token(self.connection, k) for k in token_keys]

        input_type = self.args["input_type"]
        output_type = self.args["output_type"]

        data = merge_token_data(tokens, data_type=input_type)
        token_color = self.active_color(active_tokens_key)
        token = net.create_token(data_type=output_type, data=data,
                token_color=token_color)

        return defer.succeed(token)
