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
        builder = nb.NetBuilder()

        net = builder.add_subnet(nb.EmptyNet, "hi")
        start = net.add_place("start")
        p1 = net.add_place("p1")
        p2 = net.add_place("p2")
        end = net.add_place("end")

        action = nb.ActionSpec(sn.CounterAction)
        t1 = net.add_transition("t1", action=action)
        t2 = net.add_transition("t2")

        builder.variables["x"] = "y"
        builder.variables["123"] = "456"

        start.arcs_out.add(t1)
        t1.arcs_out.add(p1)
        t1.arcs_out.add(p2)
        p1.arcs_out.add(t2)
        p2.arcs_out.add(t2)
        t2.arcs_out.add(end)

        stored_net = builder.store(self.conn)

        self.assertEqual(4, stored_net.num_places.value)
        self.assertEqual(2, stored_net.num_transitions.value)

        expected_names = ["start", "p1", "p2", "end"]
        place_names = [str(stored_net.place(x).name) for x in xrange(4)]
        self.assertEqual(expected_names, place_names)

        action = stored_net.transition(0).action
        self.assertTrue(isinstance(action, sn.CounterAction))
        self.assertEqual("t1", str(action.name))

        self.assertTrue(stored_net.transition(1).action is None)

        self.assertEqual("y", stored_net.variable("x"))
        self.assertEqual("456", stored_net.variable("123"))
        self.assertIsNone(stored_net.variable("nothing"))
