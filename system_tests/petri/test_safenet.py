#!/usr/bin/env python

import flow.petri.safenet as sn

import os
import unittest
import redis

class FakeOrchestrator(object):
    def __init__(self, conn):
        self.conn = conn
        self.services = {"orchestrator": self}

    def set_token(self, net_key, place_idx, token_key=''):
        net = sn.SafeNet(self.conn, net_key)
        net.set_token(place_idx, token_key, services=self.services)

    def notify_transition(self, net_key, trans_idx, place_idx):
        net = sn.SafeNet(self.conn, net_key)
        net.notify_transition(trans_idx, place_idx, services=self.services)

class TestBase(unittest.TestCase):
    def setUp(self):
        redis_host = os.environ['FLOW_TEST_REDIS_URL']
        self.conn = redis.Redis(redis_host)
        self.conn.flushall()
        orch = FakeOrchestrator(self.conn)
        self.services = orch.services

    def tearDown(self):
        self.conn.flushall()


class TestSafeNet(TestBase):
    def test_abstract_transition_action(self):
        act = sn.TransitionAction.create(self.conn, name="boom")
        self.assertRaises(NotImplementedError, act.execute)

    def test_no_transition_action(self):
        trans = sn._SafeTransition.create(self.conn, name="t")
        self.assertTrue(trans.action is None)

    def test_notify_transition_args(self):
        net = sn.SafeNet.create(self.conn, place_names=[], trans_actions=[],
                place_arcs_out={}, trans_arcs_out={})
        self.assertRaises(TypeError, net.notify_transition)
        self.assertRaises(TypeError, net.notify_transition, trans_idx=0)
        self.assertRaises(TypeError, net.notify_transition, trans_idx=0,
            place_idx=0)


    def test_fire_transition(self):
        places = ["place %d" % i for i in xrange(5)]
        action = sn.CounterAction.create(connection=self.conn, name="counter")
        place_arcs_out = dict((i, [0]) for i in xrange(4))
        trans_arcs_out = {0: [4]}

        net = sn.SafeNet.create(
                connection=self.conn,
                place_names=places,
                trans_actions=[action],
                place_arcs_out=place_arcs_out,
                trans_arcs_out=trans_arcs_out)

        self.assertEqual("place 0", str(net.place(0).name))
        self.assertEqual("place 1", str(net.place(1).name))
        self.assertEqual("place 2", str(net.place(2).name))
        self.assertEqual("place 3", str(net.place(3).name))
        self.assertEqual("place 4", str(net.place(4).name))

        self.assertEqual(['0'], net.place(0).arcs_out.value)
        self.assertEqual(['0'], net.place(1).arcs_out.value)
        self.assertEqual(['0'], net.place(2).arcs_out.value)
        self.assertEqual(['0'], net.place(3).arcs_out.value)
        self.assertEqual([], net.place(4).arcs_out.value)

        self.assertEqual(['0', '1', '2', '3'], net.transition(0).arcs_in.value)
        self.assertEqual(set(['0', '1', '2', '3']), net.transition(0).state.value)
        self.assertEqual(['4'], net.transition(0).arcs_out.value)
        self.assertEqual(action.key, str(net.transition(0).action_key))

        token = sn.Token.create(self.conn)
        net.set_token(0, token.key, self.services)
        self.assertEqual(0, int(action.call_count))
        net.set_token(1, token.key, self.services)
        self.assertEqual(0, int(action.call_count))
        net.set_token(2, token.key, self.services)
        self.assertEqual(0, int(action.call_count))
        net.set_token(3, token.key, self.services)
        self.assertEqual(1, int(action.call_count))

        for i in xrange(0, 3):
            net.notify_transition(0, i, self.services)
            self.assertEqual(1, int(action.call_count))

    def test_place_capacity(self):
        net = sn.SafeNet.create(self.conn, place_names=["place_1"],
                trans_actions=[], place_arcs_out={}, trans_arcs_out={})

        place_idx = 0

        token1 = sn.Token.create(self.conn)
        token2 = sn.Token.create(self.conn)

        self.assertTrue(net.marking(place_idx) is None)
        self.assertEqual({}, net.marking())

        net.set_token(place_idx, token1.key, self.services)
        self.assertEqual(token1.key, net.marking(place_idx))
        net.set_token(place_idx, token1.key, self.services)
        self.assertEqual({"0": str(token1.key)}, net.marking())

        # setting the same token twice is not an error
        self.assertEqual(token1.key, net.marking(place_idx))
        self.assertEqual({"0": str(token1.key)}, net.marking())

        self.assertRaises(sn.PlaceCapacityError, net.set_token,
            place_idx, token2.key, self.services)

if __name__ == "__main__":
    unittest.main()
