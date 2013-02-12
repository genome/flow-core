import flow.redisom as rom
import pygraphviz
import subprocess

class Node(rom.Object):
    name = rom.Property(rom.String)
    net_key = rom.Property(rom.String)

    @property
    def net(self):
        return PetriNet(self.connection, self.net_key)


class Place(Node):
    tokens = rom.Property(rom.Int)
    arcs_out = rom.Property(rom.Set)
    last_token_timestamp = rom.Property(rom.Timestamp)

    def consume_token(self):
        self.tokens.decr()

    def add_tokens(self, n, services=None):
        self.last_token_timestamp.set()
        n = int(n)
        self.tokens.incr(n)

        if not self.arcs_out:
            print "Stopping in '%s'" % self.name
        else:
            for dst_key in self.arcs_out:
                dst = Transition(self.connection, dst_key)
                if dst.notify(self.key):
                    services['orchestrator'].fire_transition(dst.key)

    def __str__(self):
        return "%s: %d" % (self.name, self.tokens)


class Transition(Node):
    actions = rom.Property(rom.List)
    pred = rom.Property(rom.Set)
    arcs_in = rom.Property(rom.Set)
    arcs_out = rom.Property(rom.Set)

    def notify(self, place_key):
        # FIXME we should probably pipeline this block
        removed, size = self.pred.remove(place_key)
        return removed and size == 0

    def fire(self, services=None):
        print "'%s' fires!" % self.name
        for action_key in self.actions:
            action = rom.get_object(self.connection, action_key)
            print "\n** running transition action\n"
            action.execute()
            print "\n** transition action complete\n"

        for dst_key in self.arcs_out:
            services['orchestrator'].add_tokens(dst_key)

        for src_key in self.arcs_in:
            src = Place(self.connection, src_key)
            src.consume_token()


class TransitionAction(rom.Object):
    def execute(self, **kwargs):
        raise NotImplementedError("In class %s: execute not implemented!" %
                self.__class__.__name__)

class ShellCommand(TransitionAction):
    cmdline = rom.Property(rom.List)
    success_place_key = rom.Property(rom.String)
    failure_place_key = rom.Property(rom.String)

    def execute(self, **kwargs):
        rv = subprocess.call(self.cmdline.value)
        if rv == 0:
            place = Place(self.connection, self.success_place_key)
        else:
            place = Place(self.connection, self.failure_place_key)

        place.add_tokens(1)


class PetriNet(Node):
    places = rom.Property(rom.Set)
    transitions = rom.Property(rom.Set)
    start_key = rom.Property(rom.String)

    def _on_create(self):
        self.start_key = self.add_place(name="start").key

    @property
    def status(self):
        active = []
        for pkey in self.places:
            place = Place(self.connection, pkey)
            if int(place.tokens) > 0:
                active.append(str(place.name))
        return ", ".join(active)

    @property
    def start(self):
        return Place(self.connection, self.start_key)

    def add_place(self, name):
        place = Place.create(self.connection, name=name, net_key=self.key,
                tokens=0)
        self.places.add(place.key)
        return place

    def add_transition(self, name, actions=None):
        transition = Transition.create(self.connection, name=name,
                net_key=self.key, actions=actions)
        self.transitions.add(transition.key)
        return transition

    def add_arc(self, src, dst):
        src.arcs_out.add(dst.key)

        if isinstance(dst, Transition):
            dst.pred.add(src.key)
            dst.arcs_in.add(src.key)

    def execute(self):
        self.start.add_tokens(1)

    def __str__(self):
        places = [Place(self.connection, x) for x in self.places]
        return "%s: <%s>" % (self.name,
            ", ".join((str(x) for x in places)))

    @property
    def graph(self):
        graph = pygraphviz.AGraph(directed=True)
        arcs = {}
        for pkey in self.places:
            p = Place(self.connection, pkey)
            arcs.setdefault(pkey, set()).update(p.arcs_out)
            if int(p.tokens) > 0:
                color = "grey"
            else:
                color = "white"
            graph.add_node(p.key, label=p.name, shape="oval",
                    style="filled", fillcolor=color)

        for tkey in self.transitions:
            t = Transition(self.connection, tkey)
            arcs.setdefault(tkey, set()).update(t.arcs_out)
            graph.add_node(t.key, label=t.name, shape="box",
                    style="filled", fillcolor="black", fontcolor="white")

        for src_key, dst_set in arcs.iteritems():
            for dst_key in dst_set:
                graph.add_edge(src_key, dst_key)

        return graph


class SuccessFailurePetriNet(PetriNet):
    success_key = rom.Property(rom.String)
    failure_key = rom.Property(rom.String)

    def _on_create(self):
        PetriNet._on_create(self)
        self.success_key = self.add_place(name="success").key
        self.failure_key = self.add_place(name="failure").key

    @property
    def status(self):
        if int(self.success.tokens) > 0:
            return "success"
        if int(self.failure.tokens) > 0:
            return "failure"

        return "running"

    @property
    def success(self):
        return Place(self.connection, self.success_key)

    @property
    def failure(self):
        return Place(self.connection, self.failure_key)

    @property
    def duration(self):
        beg = self.start.last_token_timestamp.value
        end = self.success.last_token_timestamp.value
        if beg is None or end is None:
            return None

        return float(end) - float(beg)
