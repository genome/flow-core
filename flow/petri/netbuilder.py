from flow.petri import safenet

import itertools
import pygraphviz


class Node(object):
    @property
    def node_id(self):
        return id(self)

    def __init__(self, index, name):
        self.arcs_out = set([])
        self.index = index
        self.name = name


class Place(Node):
    def __init__(self, index, name):
        Node.__init__(self, index, name)

    def graph(self, graph):
        label = self.name or " "
        graph.add_node(self.node_id, label=label)
        graph.get_node(self.node_id).attr["_type"] = "Place"

    def __repr__(self):
        return "%s(index=%r, name=%r)" % (
                self.__class__.__name__, self.index, self.name)


class ActionSpec(object):
    def __init__(self, cls, args=None, place_refs=None):
        self.cls = cls
        self.args = args
        self.place_refs = place_refs

    def create(self, conn, name=""):
        return self.cls.create(
                conn,
                name=name,
                args=self.args,
                place_refs=self.place_refs)

    def __repr__(self):
        return "%s(cls=%r, args=%r, place_refs=%r)" % (
                self.__class__.__name__, self.cls, self.args,
                self.place_refs)


class Transition(Node):
    def __init__(self, index, name="", action=None):
        Node.__init__(self, index, name)

        self.action = action
        self.arcs_in = set([])
        self.arcs_out = set([])

    def create_action(self, connection):
        if self.action:
            return self.action.create(connection, self.name)

    def graph(self, graph):
        label = self.name or " "
        graph.add_node(self.node_id, label=label, shape="box",
            fillcolor="black", style="filled", fontcolor="white")
        graph.get_node(self.node_id).attr["_type"] = "Transition"

    def __repr__(self):
        return "%s(index=%r, name=%r, action=%r)" % (
                self.__class__.__name__, self.index, self.name, self.action)


class _ClusterCounter(object):
    def __init__(self):
        self.value = 0

    def next_id(self):
        rv = "cluster_%d" % self.value
        self.value += 1
        return rv


class NetBuilder(object):
    def __init__(self):
        self.places = []
        self.transitions = []
        self.subnets = []
        self.variables = {}

        self._place_map = {}
        self._trans_map = {}

    def add_subnet(self, cls, *args, **kwargs):
        subnet = cls(self, *args, **kwargs)
        self.subnets.append(subnet)
        return subnet

    def add_place(self, name):
        index = len(self.places)
        place = Place(index, name)
        self.places.append(place)
        self._place_map[place] = index
        return place

    def add_transition(self, name="", **kwargs):
        index = len(self.transitions)
        transition = Transition(index, name, **kwargs)
        self.transitions.append(transition)
        self._trans_map[transition] = index
        return transition

    def bridge_places(self, src_place, dst_place, name=None):
        if not (isinstance(src_place, Place) and isinstance(dst_place, Place)):
            raise TypeError(
                    "bridge_places called with something other than two places")

        if src_place not in self._place_map or dst_place not in self._place_map:
            raise RuntimeError("bridge_places called with an unknown place")

        if name is None:
            name = "bridge %s -> %s" % (src_place.name, dst_place.name)

        transition = self.add_transition(name)
        src_place.arcs_out.add(transition)
        transition.arcs_out.add(dst_place)
        return transition

    def bridge_transitions(self, src_trans, dst_trans, name=None):
        if not (isinstance(src_trans, Transition) and
                isinstance(dst_trans, Transition)):
            raise TypeError(
                    "bridge_transitions called with something other than two "
                    "transitions")

        if src_trans not in self._trans_map or dst_trans not in self._trans_map:
            raise RuntimeError(
                    "bridge_transitions called with an unknown transition")

        if name is None:
            name = "bridge %s -> %s" % (src_trans.name, dst_trans.name)

        place = self.add_place(name)
        src_trans.arcs_out.add(place)
        place.arcs_out.add(dst_trans)
        return place

    def _graph(self, graph, src_node, seen):
        if src_node in seen:
            return

        src_node.graph(graph)
        seen.add(src_node)

        for dst_node in src_node.arcs_out:
            self._graph(graph, dst_node, seen)
            graph.add_edge(src_node.node_id, dst_node.node_id)

    def _graph_subnet(self, graph, subnet, seen, cluster_counter):
        cluster_id = cluster_counter.next_id()
        cluster = graph.add_subgraph(name=cluster_id, label=subnet.name,
                color="blue")

        for node in itertools.chain(subnet.places, subnet.transitions):
            node.graph(cluster)

        seen.add(subnet)

        for child in subnet.subnets:
            if child not in seen:
                self._graph_subnet(cluster, child, seen, cluster_counter)

    def graph(self, subnets=False):
        graph = pygraphviz.AGraph(directed=True)
        seen = set()

        if subnets:
            cluster_counter = _ClusterCounter()
            for subnet in self.subnets:
                if subnet not in seen:
                    self._graph_subnet(graph, subnet, seen, cluster_counter)

        for node in itertools.chain(self.places, self.transitions):
            self._graph(graph, node, seen)

        return graph

    def store(self, connection, name="net"):
        place_names = []
        place_arcs_out = {}
        for p in self.places:
            place_names.append(p.name)
            src_id = self._place_map[p]
            dst_ids = [self._trans_map[x] for x in p.arcs_out]
            place_arcs_out.setdefault(src_id, set()).update(dst_ids)

        transition_actions = []
        trans_arcs_out = {}
        for t in self.transitions:
            transition_actions.append(t.create_action(connection))
            src_id = self._trans_map[t]
            dst_ids = [self._place_map[x] for x in t.arcs_out]
            trans_arcs_out.setdefault(src_id, set()).update(dst_ids)

        net = safenet.SafeNet.create(
                connection=connection,
                name=name,
                place_names=place_names,
                trans_actions=transition_actions,
                place_arcs_out=place_arcs_out,
                trans_arcs_out=trans_arcs_out)

        for key, value in self.variables.iteritems():
            net.set_variable(key, value)

        return net


class EmptyNet(object):
    def __init__(self, builder, name):
        self.builder = builder
        self.name = name

        self.places = []
        self.transitions = []

        self.subnets = []

        self._place_set = set()
        self._trans_set = set()

    def _add_place(self, place):
        self.places.append(place)
        self._place_set.add(place)

    def _add_transition(self, transition):
        self.transitions.append(transition)
        self._trans_set.add(transition)

    def add_place(self, name=""):
        if not name:
            name = "p%d" % len(self.places)
        place = self.builder.add_place(name)
        self._add_place(place)
        return place

    def add_transition(self, name="", **kwargs):
        if not name:
            name = "t%d" % len(self.transitions)

        transition = self.builder.add_transition(name, **kwargs)
        self._add_transition(transition)
        return transition

    def bridge_places(self, src_place, dst_place, name=None):
        transition = self.builder.bridge_places(src_place, dst_place, name)
        self._add_transition(transition)
        return transition

    def bridge_transitions(self, src_trans, dst_trans, name=None):
        place = self.builder.bridge_transitions(src_trans, dst_trans, name)
        self._add_place(place)
        return place

    def add_subnet(self, cls, *args, **kwargs):
        subnet = self.builder.add_subnet(cls, *args, **kwargs)
        self.subnets.append(subnet)
        return subnet


class SuccessFailureNet(EmptyNet):
    def __init__(self, builder, name):
        EmptyNet.__init__(self, builder, name)

        self.start = self.add_place("start")
        self.success = self.add_place("success")
        self.failure = self.add_place("failure")


class ShellCommandNet(SuccessFailureNet):
    def __init__(self, builder, name, cmdline):
        SuccessFailureNet.__init__(self, builder, name)

        self.cmdline = cmdline

        self.running = self.add_place("running")
        self.on_success_place = self.add_place("on_success")
        self.on_failure_place = self.add_place("on_failure")

        action = ActionSpec(
                cls=safenet.ShellCommandAction,
                args = {"command_line": cmdline},
                place_refs=[self.on_success_place, self.on_failure_place],
                )

        self.execute = self.add_transition(name="execute", action=action)

        self.on_success = self.add_transition()
        self.on_failure = self.add_transition()

        self.start.arcs_out.add(self.execute)
        self.execute.arcs_out.add(self.running)
        self.running.arcs_out.add(self.on_success)
        self.running.arcs_out.add(self.on_failure)
        self.on_success.arcs_out.add(self.success)
        self.on_failure.arcs_out.add(self.failure)

        self.on_success_place.arcs_out.add(self.on_success)
        self.on_failure_place.arcs_out.add(self.on_failure)
