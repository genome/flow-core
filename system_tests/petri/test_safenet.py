import flow.petri.safenet as sn

import mock
import os
import redis
import sys
import test_helpers
import unittest

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


class TestBase(test_helpers.RedisTest):
    def setUp(self):
        test_helpers.RedisTest.setUp(self)
        orch = FakeOrchestrator(self.conn)
        self.services = orch.services


class TestTransition(TestBase):
    def test_default_token_merger(self):
        tokens = []
        expected = {}
        for i in xrange(10):
            expected[str(i)] = i
            tokens.append(sn.Token.create(self.conn, data={i: i}))

        token_keys = [x.key for x in tokens]
        transition = sn._SafeTransition.create(self.conn,
                active_tokens=token_keys)
        self.assertEqual(expected, transition.input_data.value)

    def test_abstract_base(self):
        merger = sn.TokenMerger.create(self.conn)
        self.assertRaises(NotImplementedError, merger.merge, [])

    def test_custom_token_merger(self):
        class PrependingTokenMerger(sn.TokenMerger):
            def merge(self, tokens):
                data = {}
                for t in tokens:
                    data.update({"x" + k: v for k, v in t.data.iteritems()})
                return data

        tokens = []
        expected = {}
        for i in xrange(10):
            expected["x" + str(i)] = i
            tokens.append(sn.Token.create(self.conn, data={i: i}))

        token_keys = [x.key for x in tokens]
        merger = PrependingTokenMerger.create(self.conn)
        transition = sn._SafeTransition.create(self.conn,
                active_tokens=token_keys,
                token_merger_key=merger.key)
        self.assertEqual(expected, transition.input_data.value)


class TestSafeNet(TestBase):
    def test_no_connection(self):
        self.assertRaises(TypeError, sn.SafeNet.create, None)

    def test_abstract_transition_action(self):
        act = sn.TransitionAction.create(self.conn, name="boom")
        self.assertRaises(NotImplementedError, act.execute, input_data={},
                net=None, services=None)

    def test_attributes(self):
        net = sn.SafeNet.create(connection=self.conn)
        env = {"PATH": "/bin:/usr/bin", "USER": "flow"}
        net.set_attribute("environment", env)
        self.assertEqual(env, net.attribute("environment"))

    def test_places(self):
        action = sn.CounterAction.create(connection=self.conn, name="counter")
        net = sn.SafeNet.create(
                connection=self.conn,
                place_names=["a", "b", "c"],
                trans_actions = [action],
                place_arcs_out={0: [0]}, # a -> t1
                trans_arcs_out={0: [1, 2]}, # t1 -> b, c
                )

        self.assertEqual(3, net.num_places)
        self.assertEqual("a", str(net.place(0).name))
        self.assertEqual("b", str(net.place(1).name))
        self.assertEqual("c", str(net.place(2).name))

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

    def test_graph(self):
        net = sn.SafeNet.create(self.conn, place_names=["p1", "p2"],
                trans_actions=[None], place_arcs_out={0: set([0])},
                trans_arcs_out={0: set([1])})

        graph = net.graph()
        nodes = graph.nodes()
        edges = graph.edges()
        self.assertEqual(3, len(nodes))
        self.assertEqual(2, len(edges))


class TestTransitionActions(TestBase):
    def test_argument_encoding(self):
        arguments = {
            "integer": 7,
            "float": 1.23,
            "string": "hello",
            "list": [1, 2, "three"],
            "hash": {"x": "y"},
        }
        action = sn.CounterAction.create(connection=self.conn, args=arguments)

        self.assertEqual(action.args.value, arguments)

    def test_shell_command_action(self):
        success_place_id = 1
        failure_place_id = 2

        good_cmdline = [sys.executable, "-c", "import sys; sys.exit(0)"]
        fail_cmdline = [sys.executable, "-c", "import sys; sys.exit(1)"]

        action = sn.ShellCommandAction.create(
            connection=self.conn,
            name="TestAction",
            args={"command_line": good_cmdline},
            place_refs=[success_place_id, failure_place_id],
            )

        self.assertEqual(good_cmdline, action.args["command_line"])

        orchestrator = mock.MagicMock()
        services = {"orchestrator": orchestrator}
        net = mock.MagicMock()
        net.key = "netkey!"

        input_data = {}
        action.execute(input_data, net, services)
        orchestrator.set_token.assert_called_with(
                net.key, success_place_id, token_key=mock.ANY)

        action.args["command_line"] = fail_cmdline
        orchestrator.reset_mock()

        action.execute(input_data, net, services)
        orchestrator.set_token.assert_called_with(
                    net.key, failure_place_id, token_key=mock.ANY)


if __name__ == "__main__":
    unittest.main()
