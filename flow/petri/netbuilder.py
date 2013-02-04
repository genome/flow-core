import pygraphviz
import safenet as sn
from copy import deepcopy

def _make_transition_action(conn, transition):
    if transition.action_class is None:
        return None

    return transition.action_class.create(
            conn,
            name=transition.name,
            args=transition.action_args,
            place_refs=transition.place_refs)


class Node(object):
    def __init__(self):
        self.arcs_out = set([])


class Place(Node):
    def __init__(self, name):
        Node.__init__(self)
        self.name = name


class Transition(Node):
    def __init__(self, name, action_class=None, action_args=None, place_refs=None):
        Node.__init__(self)
        self.name = name
        self.action_class = action_class
        self.action_args = action_args
        self.place_refs = place_refs
        self.arcs_out = set([])


class Net(object):
    def __init__(self, name):
        self.name = name
        self.places = []
        self.transitions = []
        self.place_arcs_out = {}
        self.trans_arcs_out = {}

    def add_place(self, place):
        self.places.append(place)
        return len(self.places) - 1

    def add_transition(self, transition):
        self.transitions.append(transition)
        return len(self.transitions) - 1

    def add_place_arc_out(self, src, dst):
        self.place_arcs_out.setdefault(src, set()).add(dst)

    def add_trans_arc_out(self, src, dst):
       self.trans_arcs_out.setdefault(src, set()).add(dst)

    def graph(self):
        graph = pygraphviz.AGraph(directed=True)

        for i, p in enumerate(self.places):
            graph.add_node("p%d" % i, label=p.name)

        for i, t in enumerate(self.transitions):
            graph.add_node("t%d" % i, label=t.name, shape="box",
                    style="filled", fillcolor="black", fontcolor="white")

        for src, dst_set in self.place_arcs_out.iteritems():
            for dst in dst_set:
                pid = "p%d" % src
                tid = "t%d" % dst
                graph.add_edge(pid, tid)

        for src, dst_set in self.trans_arcs_out.iteritems():
            for dst in dst_set:
                tid = "t%d" % src
                pid = "p%d" % dst
                graph.add_edge(tid, pid)

        return graph

    def store(self, connection):
        transition_actions = [_make_transition_action(connection, x)
                              for x in self.transitions]
        return sn.SafeNet.create(
                connection=connection,
                name=self.name,
                place_names=self.places,
                trans_actions=transition_actions,
                place_arcs_out=self.place_arcs_out,
                trans_arcs_out=self.trans_arcs_out)

    def update(self, other, place_bridges):
        other = deepcopy(other)

        place_offset = len(self.places)
        trans_offset = len(self.transitions)
        self.places.extend(other.places)

        for t in other.transitions:
            if t.place_refs:
                t.place_refs = [x + place_offset for x in t.place_refs]
            self.transitions.append(t)

        for src, dst_set in other.place_arcs_out.iteritems():
            src += place_offset
            for dst in dst_set:
                dst += trans_offset
                self.place_arcs_out.setdefault(src, set()).add(dst)

        for src, dst_set in other.trans_arcs_out.iteritems():
            src += trans_offset
            for dst in dst_set:
                dst += place_offset
                self.trans_arcs_out.setdefault(src, set()).add(dst)

        for src, dst in place_bridges.iteritems():
            dst += place_offset
            tid = self.add_transition(Transition(name="bridge"))
            self.place_arcs_out.setdefault(src, set()).add(tid)
            self.trans_arcs_out.setdefault(tid, set()).add(dst)
