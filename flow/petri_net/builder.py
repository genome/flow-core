from flow.petri_net.future import FutureBarrierTransition
from flow.petri_net.future import FutureBasicTransition
from flow.petri_net.net import Net
from flow.petri_net.place import Place
from flow.petri_net.transitions.barrier import BarrierTransition
from flow.petri_net.transitions.basic import BasicTransition

import logging


LOG = logging.getLogger(__name__)


class Builder(object):
    def __init__(self, connection):
        self.connection = connection

    def store(self, future_net, variables, constants):
        future_places, future_transitions = gather_nodes(future_net)

        stored_net = self.create_stored_net(future_net, variables, constants)

        for place, index in future_places.iteritems():
            self.store_place(stored_net, place, index, future_transitions)

        for transition, index in future_transitions.iteritems():
            self.store_transition(stored_net, transition, index, future_places)

        stored_net.num_places = len(future_places)
        stored_net.num_transitions = len(future_transitions)

        return stored_net


    def create_stored_net(self, future_net, variables, constants):
        stored_net = Net.create(self.connection)
        stored_net.name = future_net.name

        stored_net.variables = variables

        for k, v in constants.iteritems():
            stored_net.set_constant(k, v)

        return stored_net


    def store_place(self, stored_net, future_place, index, future_transitions):
        key = stored_net.place_key(index)
        stored_place = Place.create(self.connection, key,
                name=future_place.name, index=index)

        for arc in future_place.arcs_in:
            stored_place.arcs_in.append(future_transitions[arc])

        for arc in future_place.arcs_out:
            stored_place.arcs_out.append(future_transitions[arc])

        return stored_place

    def store_transition(self, stored_net, future_transition,
            index, future_places):
        if isinstance(future_transition, FutureBasicTransition):
            cls = BasicTransition
        elif isinstance(future_transition, FutureBarrierTransition):
            cls = BarrierTransition
        else:
            raise RuntimeError('Unknown FutureTransition')

        key = stored_net.transition_key(index)
        stored_transition = cls.create(self.connection, key,
                name=future_transition.name, index=index)

        if future_transition.action is not None:
            stored_transition.set_action(future_transition.action.cls,
                    convert_action_args(future_transition.action.args,
                        future_places))


        for arc in future_transition.arcs_in:
            stored_transition.arcs_in.append(future_places[arc])

        for arc in future_transition.arcs_out:
            stored_transition.arcs_out.append(future_places[arc])

        return stored_transition


def gather_nodes(future_net):
    future_places = {}
    future_transitions = {}

    _gather_nodes_recursive(future_net, future_places, future_transitions)

    return future_places, future_transitions

def _gather_nodes_recursive(future_net, future_places, future_transitions):
    for p in future_net.places:
        future_places[p] = len(future_places)

    for t in future_net.transitions:
        future_transitions[t] = len(future_transitions)

    for subnet in future_net.subnets:
        _gather_nodes_recursive(subnet, future_places, future_transitions)


def convert_action_args(orig_args, substitutions):
    args = {}
    for name, value in orig_args.iteritems():
        try:
            args[name] = substitutions.get(value, value)
        except TypeError:
            # value is unhashable type
            args[name] = value

    return args
