import flow.petri.safenet as sn

from test_helpers import RedisTest, FakeOrchestrator
import mock
import os
import redis
import sys
import unittest


class TestBase(RedisTest):
    def setUp(self):
        RedisTest.setUp(self)
        orch = FakeOrchestrator(self.conn)
        self.services = orch.services


class TestTransition(TestBase):
    def test_token_merger(self):
        tokens = []
        expected_all = {}
        expected_outputs = {}

        for i in xrange(10):
            expected_all[str(i)] = i
            token = sn.Token.create(self.conn, data={i: i})
            token.data_type = "output"
            tokens.append(token)

        expected_outputs = dict(expected_all)
        del expected_outputs['4']
        tokens[4].data_type = "input"

        merged = sn.merge_token_data(tokens)
        self.assertEqual(expected_all, merged)

        merged = sn.merge_token_data(tokens, "output")
        self.assertEqual(expected_outputs, merged)


class TestSafeNet(TestBase):
    def test_no_connection(self):
        self.assertRaises(TypeError, sn.SafeNet.create, None)

    def test_abstract_transition_action(self):
        act = sn.TransitionAction.create(self.conn, name="boom")
        self.assertRaises(NotImplementedError, act.execute, net=None,
                services=None, active_tokens_key=None)

    def test_constants(self):
        net = sn.SafeNet.create(connection=self.conn)
        env = {"PATH": "/bin:/usr/bin", "USER": "flow"}
        net.set_constant("environment", env)
        self.assertEqual(env, net.constant("environment"))
        self.assertRaises(TypeError, net.set_constant, "environment", 10)

        user_id = 1234
        no_user_id = net.constant("user_id")
        self.assertIsNone(no_user_id)
        net.set_constant("user_id", user_id)
        self.assertRaises(TypeError, net.set_constant, "user_id", 10)
        self.assertEqual(1234, net.constant("user_id"))

    def test_variables(self):
        net = sn.SafeNet.create(connection=self.conn)

        foo = net.variable("foo")
        self.assertIsNone(foo)
        net.set_variable("foo", 123)
        self.assertEqual(123, net.variable("foo"))

        bar = net.variable("bar")
        self.assertIsNone(bar)
        net.set_variable("bar", {"x": "y"})
        self.assertEqual({"x": "y"}, net.variable("bar"))

        baz = net.variable("baz")
        self.assertIsNone(baz)
        net.set_variable("bar", [1, 2, 3])
        self.assertEqual([1, 2, 3], net.variable("bar"))

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

        # Test that marked places show up in red
        token = sn.Token.create(self.conn)
        rv = net._set_token(self.conn, keys=[net.subkey("marking")],
                args=[0, token.key])

        marked_graph = net.graph()
        nodes = marked_graph.nodes()
        p1 = [x for x in nodes if x.attr["label"] == "p1"]
        p2 = [x for x in nodes if x.attr["label"] == "p2"]

        self.assertEqual(1, len(p1))
        self.assertEqual(1, len(p2))
        p1 = p1[0]
        p2 = p2[0]

        self.assertEqual("red", p1.attr["fillcolor"])
        self.assertEqual("white", p2.attr["fillcolor"])


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

        active_tokens_key = "x"
        action.execute(active_tokens_key, net, services)
        orchestrator.set_token.assert_called_with(
                net.key, success_place_id, token_key=mock.ANY)

        action.args["command_line"] = fail_cmdline
        orchestrator.reset_mock()

        action.execute(active_tokens_key, net, services)
        orchestrator.set_token.assert_called_with(
                    net.key, failure_place_id, token_key=mock.ANY)


if __name__ == "__main__":
    unittest.main()
