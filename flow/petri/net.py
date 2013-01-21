import flow.orchestrator.redisom as rom
import pygraphviz
from collections import defaultdict
import subprocess
from uuid import uuid4

class Node(rom.Object):
    name = rom.Property(rom.Scalar)
    net_key = rom.Property(rom.Scalar)

    @property
    def net(self):
        return rom.get_object(self._connection, self.net_key)


class Place(Node):
    tokens = rom.Property(rom.Scalar)
    arcs_out = rom.Property(rom.Hash)

    def consume_tokens(self, n):
        n = int(n)
        print "'%s' loses %d token(s)" % (self.name, n)
        self.tokens.increment(-n)

    def add_tokens(self, n):
        n = int(n)
        print "'%s' gains %d token(s)" % (self.name, int(n))
        self.tokens.increment(n)

        if not self.arcs_out:
            print "Stopping in '%s'" % self.name
        else:
            for dst_key, multiplicity in self.arcs_out.iteritems():
                dst = rom.get_object(self._connection, dst_key)
                print "'%s' tells '%s' that it just got %d token(s)" %(
                        self.name, dst.name, n)
                if dst.notify(self.key, n):
                    dst.fire()

    def __str__(self):
        return "%s: %d" % (self.name, self.tokens)


class Transition(Node):
    actions = rom.Property(rom.List)
    pred = rom.Property(rom.Hash)
    arcs_in = rom.Property(rom.Hash)
    arcs_out = rom.Property(rom.Hash)

    def notify(self, place_key, num_tokens):
        # FIXME we should probably pipeline this block
        n = self.pred.increment(place_key, -num_tokens)
        if n == 0:
            del self.pred[place_key]
        ready = len(self.pred) == 0

        if ready:
            print "'%s' is pleased!" % self.name
        else:
            print "'%s' is indifferent." % self.name
        return ready

    def fire(self):
        print "'%s' fires!" % self.name
        for src_key, multiplicity in self.arcs_in.iteritems():
            src = rom.get_object(self._connection, src_key)
            src.consume_tokens(multiplicity)

        for action_key in self.actions:
            action = rom.get_object(self._connection, action_key)
            print "\n** running transition action\n"
            action.execute()
            print "\n** transition action complete\n"

        for dst_key, multiplicity in self.arcs_out.iteritems():
            dst = rom.get_object(self._connection, dst_key)
            dst.add_tokens(multiplicity)


class TransitionAction(rom.Object):
    def execute(self, **kwargs):
        raise NotImplementedError("In class %s: execute not implemented!" %
                self.__class__.__name__)

class ShellCommand(TransitionAction):
    cmdline = rom.Property(rom.List)
    success_place_key = rom.Property(rom.Scalar)
    failure_place_key = rom.Property(rom.Scalar)

    def execute(self, **kwargs):
        rv = subprocess.call(self.cmdline.value)
        if rv == 0:
            place = rom.get_object(self._connection, self.success_place_key)
        else:
            place = rom.get_object(self._connection, self.failure_place_key)

        place.add_tokens(1)


class PetriNet(Node):
    places = rom.Property(rom.Set)
    transitions = rom.Property(rom.Set)
    start_key = rom.Property(rom.Scalar)

    def _on_create(self):
        self.start_key = self.add_place(name="start").key

    @property
    def start(self):
        print "Looking up start node", self.start_key
        return rom.get_object(self._connection, self.start_key)

    def add_place(self, name):
        place = Place.create(self._connection, name=name, net_key=self.key,
                tokens=0)
        self.places.add(place.key)
        return place

    def add_transition(self, name, actions=None):
        transition = Transition.create(self._connection, name=name,
                net_key=self.key, actions=actions)
        self.transitions.add(transition.key)
        return transition

    def add_arc(self, src, dst, multiplicity=1):
        src.arcs_out.increment(dst.key, multiplicity)

        if isinstance(dst, Transition):
            dst.pred[src.key] = multiplicity
            dst.arcs_in.increment(src.key, multiplicity)

    def execute(self):
        print "Starting node", self.start_key, self.start
        self.start.add_tokens(1)

    def __str__(self):
        places = [rom.get_object(self._connection, x) for x in self.places]
        return "%s: <%s>" % (self.name,
            ", ".join((str(x) for x in places)))

    @property
    def graph(self):
        graph = pygraphviz.AGraph(directed=True)
        arcs = {}
        for pkey in self.places:
            p = rom.get_object(self._connection, pkey)
            arcs.setdefault(pkey, set()).update(p.arcs_out.keys())
            if int(p.tokens) > 0:
                color = "grey"
            else:
                color = "white"
            graph.add_node(p.key, label=p.name, shape="oval",
                    style="filled", fillcolor=color)

        for tkey in self.transitions:
            t = rom.get_object(self._connection, tkey)
            arcs.setdefault(tkey, set()).update(t.arcs_out.keys())
            graph.add_node(t.key, label=t.name, shape="box",
                    style="filled", fillcolor="black", fontcolor="white")

        for src_key, dst_set in arcs.iteritems():
            for dst_key in dst_set:
                graph.add_edge(src_key, dst_key)

        return graph

class SuccessFailurePetriNet(PetriNet):
    success_key = rom.Property(rom.Scalar)
    failure_key = rom.Property(rom.Scalar)

    def _on_create(self):
        PetriNet._on_create(self)
        self.success_key = self.add_place(name="success").key
        self.failure_key = self.add_place(name="failure").key

    @property
    def success(self):
        return rom.get_object(self._connection, self.success_key)

    @property
    def failure(self):
        return rom.get_object(self._connection, self.failure_key)
