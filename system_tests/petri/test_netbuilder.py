import flow.petri.netbuilder as nb
import flow.petri.safenet as sn

import mock
import os
import redis
import sys
import unittest
import test_helpers


class TestNetBuilder(test_helpers.RedisTest):
    def test_store(self):
        net = nb.Net("net")
        places = []
        for x in xrange(4):
            places.append(net.add_place("p%d" % x))

        t0 = net.add_transition(nb.Transition(
                name="t0",
                action_class=sn.CounterAction))

        t1 = net.add_transition(nb.Transition(name="t1"))

        net.add_place_arc_out(places[0], t1)
        net.add_trans_arc_out(t0, places[1])
        net.add_trans_arc_out(t0, places[2])
        net.add_trans_arc_out(places[1], t1)
        net.add_trans_arc_out(places[2], t1)
        net.add_trans_arc_out(t1, places[3])

        stored_net = net.store(self.conn)

        self.assertEqual(4, stored_net.num_places)
        self.assertEqual(2, stored_net.num_transitions)

        for x in xrange(4):
            self.assertEqual("p%d" % x, str(stored_net.place(x).name))

        action = stored_net.transition(0).action
        self.assertTrue(isinstance(action, sn.CounterAction))
        self.assertEqual("t0", str(action.name))

        self.assertTrue(stored_net.transition(1).action is None)

