from flow import petri
import flow.petri.netbuilder as nb
import flow.redisom as rom

import mock
import os
import redis
import sys
import unittest
from test_helpers import RedisTest, FakeOrchestrator

class TestBase(RedisTest):
    def setUp(self):
        RedisTest.setUp(self)
        orch = FakeOrchestrator(self.conn)
        self.service_interfaces = orch.service_interfaces

class _TestBody(object):
    net_type = None

    def test_children(self):
        builder = nb.NetBuilder(net_type=self.net_type)

        idx, c1 = builder.add_child_builder(petri.Net)
        self.assertEqual(0, idx)
        c1.add_place("c1")

        idx, c2 = builder.add_child_builder(petri.SafeNet)
        self.assertEqual(1, idx)
        c2.add_place("c2")

        idx, c1c1 = c1.add_child_builder(petri.SafeNet)
        self.assertEqual(0, idx)
        c1c1.add_place("c1c1")

        idx, c2c1 = c2.add_child_builder(petri.Net)
        self.assertEqual(0, idx)
        c2c1.add_place("c2c1")

        stored_net = builder.store(self.conn)

        self.assertIsNone(stored_net.parent_net)
        self.assertEqual(2, len(stored_net.child_net_keys))

        stored_children = [stored_net.child_net(x) for x in (0, 1)]

        self.assertIsInstance(stored_children[0], petri.Net)
        self.assertIsInstance(stored_children[1], petri.SafeNet)

        self.assertEqual(1, stored_children[0].num_places.value)
        self.assertEqual(1, stored_children[1].num_places.value)
        self.assertEqual("c1", stored_children[0].place(0).name.value)
        self.assertEqual("c2", stored_children[1].place(0).name.value)

        other_class = {petri.Net: petri.SafeNet, petri.SafeNet: petri.Net}
        for i, c in enumerate(stored_children):
            self.assertEqual(stored_net.key, c.parent_net.key)
            self.assertEqual(1, len(c.child_net_keys))
            child = c.child_net(0)
            self.assertIsInstance(child, other_class[c.__class__])
            expected_place_name = "c%dc1" % (i+1)
            self.assertEqual(1, child.num_places.value)
            self.assertEqual(expected_place_name, child.place(0).name.value)

    def test_store(self):
        builder = nb.NetBuilder(net_type=self.net_type)

        net = builder.add_subnet(nb.EmptyNet, "hi")
        start = net.add_place("start")
        p1 = net.add_place("p1")
        p2 = net.add_place("p2")
        end = net.add_place("end")

        action = nb.ActionSpec(petri.CounterAction)
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
        self.assertTrue(isinstance(action, petri.CounterAction))
        self.assertEqual("t1", str(action.name))

        self.assertTrue(stored_net.transition(1).action is None)

        self.assertEqual("y", stored_net.variable("x"))
        self.assertEqual("456", stored_net.variable("123"))
        self.assertIsNone(stored_net.variable("nothing"))


class TestNetBuilder(_TestBody, TestBase):
    net_type = petri.Net

    def test_execute_net(self):
        builder = nb.NetBuilder(net_type=petri.Net)
        in1 = builder.add_place("in1")
        in2 = builder.add_place("in2")
        p11 = builder.add_place("p11")
        p12 = builder.add_place("p12")
        p21 = builder.add_place("p21")
        p22 = builder.add_place("p22")

        action = nb.ActionSpec(petri.CounterAction)
        tstart = builder.add_transition("tstart", action=action)
        tend = builder.add_transition("tend", action=action)

        in1.arcs_out.add(tstart)
        in2.arcs_out.add(tstart)
        tstart.arcs_out.add(p11)
        tstart.arcs_out.add(p21)

        builder.bridge_places(p11, p12, action=action)
        builder.bridge_places(p21, p22, action=action)

        p12.arcs_out.add(tend)
        p22.arcs_out.add(tend)

        stored_net = builder.store(self.conn)
        stored_net.set_num_token_colors(3)

        tokens = [petri.Token.create(self.conn, color_idx=x)
                for x in xrange(3)]

        for x in xrange(3):
            stored_net.put_token(0, tokens[x])
            stored_net.notify_place(0, token_color=tokens[x].color_idx,
                    service_interfaces=self.service_interfaces)

        for x in xrange(3):
            stored_net.put_token(1, tokens[x])
            stored_net.notify_place(1, token_color=tokens[x].color_idx,
                    service_interfaces=self.service_interfaces)

        for x in xrange(stored_net.num_transitions):
            self.assertEqual(3, stored_net.transition(x).action.call_count.value)
