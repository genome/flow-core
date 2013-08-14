class FutureNet(object):
    def __init__(self, name=''):
        self.name = name
        self.places = set()
        self.subnets = set()
        self.transitions = set()

    def _add_transition(self, cls, name, action):
        trans = cls(name, action)
        self.transitions.add(trans)
        return trans

    def add_barrier_transition(self, name='', action=None):
        return self._add_transition(FutureBarrierTransition, name, action)

    def add_basic_transition(self, name='', action=None):
        return self._add_transition(FutureBasicTransition, name, action)

    def add_place(self, name=''):
        place = FuturePlace(name=name)
        self.places.add(place)
        return place

    def add_subnet(self, cls, **kwargs):
        subnet = cls(**kwargs)
        self.subnets.add(subnet)
        return subnet


    def bridge_places(self, src, dest, name=''):
        trans = self.add_basic_transition(name)
        trans.add_arc_in(src)
        trans.add_arc_out(dest)

        return trans

    def bridge_transitions(self, src, dest, name=''):
        place = self.add_place(name)
        place.add_arc_in(src)
        place.add_arc_out(dest)

        return place

    def observe_transition(self, trans, observer_action, name=''):
        observer = self.add_basic_transition(name=name, action=observer_action)
        self.bridge_transitions(trans, observer)
        return observer

    def split_place(self, src, dest_list, name=''):
        trans = self.add_basic_transition(name)
        trans.add_arc_in(src)
        for dest in dest_list:
            trans.add_arc_out(dest)

        return trans

    def join_transitions_as_or(self, destination, sources, name=''):
        place = self.add_place(name=name)
        place.add_arc_out(destination)
        for source in sources:
            source.add_arc_out(place)

        return place


class FutureNode(object):
    def __init__(self, name=''):
        self.name = name
        self.arcs_in = set()
        self.arcs_out = set()

    def add_arc_in(self, other):
        self.arcs_in.add(other)
        other.arcs_out.add(self)

    def add_arc_out(self, other):
        other.add_arc_in(self)


class FuturePlace(FutureNode):
    def add_arc_in(self, transition):
        assert isinstance(transition, FutureTransition)
        FutureNode.add_arc_in(self, transition)

    def add_arc_out(self, transition):
        assert isinstance(transition, FutureTransition)
        FutureNode.add_arc_out(self, transition)


class FutureTransition(FutureNode):
    def __init__(self, name='', action=None):
        FutureNode.__init__(self, name)
        self.action = action

    def add_arc_in(self, place):
        assert isinstance(place, FuturePlace)
        FutureNode.add_arc_in(self, place)

    def add_arc_out(self, place):
        assert isinstance(place, FuturePlace)
        FutureNode.add_arc_out(self, place)


class FutureBasicTransition(FutureTransition):
    pass


class FutureBarrierTransition(FutureTransition):
    pass


class FutureAction(object):
    def __init__(self, cls=None, **kwargs):
        self.cls = cls
        self.args = kwargs
