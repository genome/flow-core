from flow import petri

# netbuilder makes the "copy net" test easier
import flow.petri.netbuilder as nb

from test_helpers import RedisTest, FakeOrchestrator
import mock
import os
import pwd
import redis
import sys
import unittest


class TestBase(RedisTest):
    def setUp(self):
        RedisTest.setUp(self)
        orch = FakeOrchestrator(self.conn)
        self.service_interfaces = orch.service_interfaces


class TestTransition(TestBase):
    def test_token_merger(self):
        tokens = []
        expected_all = {}
        expected_outputs = {}

        for i in xrange(10):
            expected_all[str(i)] = i
            token = petri.Token.create(self.conn, data={i: i})
            token.data_type = "output"
            tokens.append(token)

        expected_outputs = dict(expected_all)
        del expected_outputs['4']
        tokens[4].data_type = "input"

        merged = petri.merge_token_data(tokens)
        self.assertEqual(expected_all, merged)

        merged = petri.merge_token_data(tokens, "output")
        self.assertEqual(expected_outputs, merged)


class TestSafeNet(TestBase):
    def test_no_connection(self):
        self.assertRaises(TypeError, petri.SafeNet.create, None)

    def test_constants(self):
        net = petri.SafeNet.create(connection=self.conn)
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

    def test_copy_constants(self):
        net1 = petri.SafeNet.create(connection=self.conn)

        nested = {"one": [2, "three", {"four": 5}]}

        net1.set_constant("string", "hello")
        net1.set_constant("number", 32)
        net1.set_constant("complex", nested)

        self.assertEqual("hello", net1.constant("string"))
        self.assertEqual(32, net1.constant("number"))
        self.assertEqual(nested, net1.constant("complex"))

        net2 = petri.SafeNet.create(connection=self.conn)

        self.assertIsNone(net2.constant("string"))
        self.assertIsNone(net2.constant("number"))
        self.assertIsNone(net2.constant("complex"))

        net2.copy_constants_from(net1)

        self.assertEqual("hello", net2.constant("string"))
        self.assertEqual(32, net2.constant("number"))
        self.assertEqual(nested, net2.constant("complex"))

        self.assertEqual("hello", net1.constant("string"))
        self.assertEqual(32, net1.constant("number"))
        self.assertEqual(nested, net1.constant("complex"))

    def test_capture_environment(self):
        uid = os.getuid()
        gid = os.getgid()
        user_name = pwd.getpwuid(uid).pw_name

        net = petri.SafeNet.create(connection=self.conn)
        net.capture_environment()
        self.assertEqual(os.environ.data, net.constant("environment"))
        self.assertEqual(uid, net.constant("user_id"))
        self.assertEqual(gid, net.constant("group_id"))
        self.assertEqual(user_name, net.constant("user_name"))
        self.assertEqual(os.path.realpath(os.path.curdir),
                net.constant("working_directory"))

    def test_variables(self):
        net = petri.SafeNet.create(connection=self.conn)

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
        action = petri.CounterAction.create(connection=self.conn, name="counter")
        net = petri.SafeNet.create(
                connection=self.conn,
                place_names=["a", "b", "c"],
                trans_actions = [action],
                place_arcs_out={0: [0]}, # a -> t1
                trans_arcs_out={0: [1, 2]}, # t1 -> b, c
                )

        self.assertEqual(3, net.num_places.value)
        self.assertEqual("a", str(net.place(0).name))
        self.assertEqual("b", str(net.place(1).name))
        self.assertEqual("c", str(net.place(2).name))

    def test_place_observers(self):
        net = petri.SafeNet.create(
                connection=self.conn,
                place_names=["a"],
                trans_actions = [],
                place_arcs_out={},
                trans_arcs_out={},
                )

        p = net.place(0)
        p.add_observer('exchange 1', 'routing key 1', 'body 1')
        p.add_observer('exchange 2', 'routing key 2', 'body 2')

        token = petri.Token.create(self.conn)
        net.set_token(0, token.key, self.service_interfaces)

        orch = self.service_interfaces['orchestrator']
        self.assertEqual(orch.place_entry_observed.call_args_list,
                [mock.call({u'exchange': u'exchange 1',
                            u'routing_key': u'routing key 1',
                            u'body': u'body 1'}),
                 mock.call({u'exchange': u'exchange 2',
                            u'routing_key': u'routing key 2',
                            u'body': u'body 2'})])

    def test_no_transition_action(self):
        trans = petri.SafeNet.transition_class.create(self.conn, name="t")
        self.assertTrue(trans.action is None)

    def test_notify_transition_args(self):
        net = petri.SafeNet.create(self.conn, place_names=[], trans_actions=[],
                place_arcs_out={}, trans_arcs_out={})
        self.assertRaises(TypeError, net.notify_transition)
        self.assertRaises(TypeError, net.notify_transition, trans_idx=0)
        self.assertRaises(TypeError, net.notify_transition, trans_idx=0,
            place_idx=0)

    def test_fire_transition(self):
        places = ["place %d" % i for i in xrange(5)]
        action = petri.CounterAction.create(connection=self.conn, name="counter")
        place_arcs_out = dict((i, [0]) for i in xrange(4))
        trans_arcs_out = {0: [4]}

        net = petri.SafeNet.create(
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

        token = petri.Token.create(self.conn)
        net.set_token(0, token.key, self.service_interfaces)
        self.assertEqual(0, int(action.call_count))
        net.set_token(1, token.key, self.service_interfaces)
        self.assertEqual(0, int(action.call_count))
        net.set_token(2, token.key, self.service_interfaces)
        self.assertEqual(0, int(action.call_count))
        net.set_token(3, token.key, self.service_interfaces)
        self.assertEqual(1, int(action.call_count))

        for i in xrange(0, 3):
            net.notify_transition(0, i, self.service_interfaces)
            self.assertEqual(1, int(action.call_count))

    def test_place_capacity(self):
        net = petri.SafeNet.create(self.conn, place_names=["place_1"],
                trans_actions=[], place_arcs_out={}, trans_arcs_out={})

        place_idx = 0

        token1 = petri.Token.create(self.conn)
        token2 = petri.Token.create(self.conn)

        self.assertTrue(net.marking.get(place_idx) is None)
        self.assertEqual({}, net.marking.value)

        net.set_token(place_idx, token1.key, self.service_interfaces)
        self.assertEqual(token1.key, net.marking.get(place_idx))
        net.set_token(place_idx, token1.key, self.service_interfaces)
        self.assertEqual({"0": str(token1.key)}, net.marking.value)

        # setting the same token twice is not an error
        self.assertEqual(token1.key, net.marking.get(place_idx))
        self.assertEqual({"0": str(token1.key)}, net.marking.value)

        self.assertRaises(petri.PlaceCapacityError, net.set_token,
            place_idx, token2.key, self.service_interfaces)

    def test_copy(self):
        builder = nb.NetBuilder()
        builder.add_subnet(nb.ShellCommandNet, "shellcmd", ["ls", "-al"])
        builder.variables["hi"] = "there"
        builder.variables["number"] = "47"
        net1 = builder.store(self.conn)
        net1.capture_environment()

        net1.input_places = {"start": 0}

        pre_copy_keys = self.conn.keys()
        new_key = petri.make_net_key()
        net2 = net1.copy(new_key)
        post_copy_keys = self.conn.keys()

        key_len = len(net1.key)
        self.assertEqual(len(net2.key), key_len)

        net1_keys_pre = []
        net1_keys_post = []
        net2_keys = []
        other_keys_pre =  []
        other_keys_post =  []

        for key in pre_copy_keys:
            if key.startswith(net1.key):
                net1_keys_pre.append(key[key_len:])
            else:
                other_keys_pre.append(key)

        for key in post_copy_keys:
            if key.startswith(net1.key):
                net1_keys_post.append(key[key_len:])
            elif key.startswith(net2.key):
                net2_keys.append(key[key_len:])
            else:
                other_keys_post.append(key)

        self.assertItemsEqual(net1_keys_pre, net1_keys_post)
        self.assertItemsEqual(net1_keys_post, net2_keys)

        eq_attrs = ["num_places", "num_transitions", "marking", "variables",
                "_constants", "input_places", "output_places"]

        for attr in eq_attrs:
            value1 = getattr(net1, attr).value
            value2 = getattr(net2, attr).value
            self.assertEqual(value1, value2,
                    "Attribute %s not copied correctly" % attr)

    def test_graph(self):
        net = petri.SafeNet.create(self.conn, place_names=["p1", "p2"],
                trans_actions=[None], place_arcs_out={0: set([0])},
                trans_arcs_out={0: set([1])})

        graph = net.graph()
        nodes = graph.nodes()
        edges = graph.edges()
        self.assertEqual(3, len(nodes))
        self.assertEqual(2, len(edges))

        # Test that marked places show up in red
        token = petri.Token.create(self.conn)
        rv = net._set_token(keys=[net.subkey("marking")],
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
    def test_abstract_transition_action(self):
        act = petri.TransitionAction.create(self.conn, name="boom")
        self.assertIsNone(act.input_data(active_tokens_key="x", net=None))

        self.assertRaises(NotImplementedError, act.execute, net=None,
                service_interfaces=None, active_tokens_key=None)

    def test_argument_encoding(self):
        arguments = {
            "integer": 7,
            "float": 1.23,
            "string": "hello",
            "list": [1, 2, "three"],
            "hash": {"x": "y"},
        }
        action = petri.CounterAction.create(connection=self.conn, args=arguments)

        self.assertEqual(action.args.value, arguments)

    def test_shell_command_action(self):
        success_place_id = 1
        failure_place_id = 2

        good_cmdline = [sys.executable, "-c", "import sys; sys.exit(0)"]
        fail_cmdline = [sys.executable, "-c", "import sys; sys.exit(1)"]

        action = petri.ShellCommandAction.create(
                connection=self.conn,
                name="TestAction",
                args={"command_line": good_cmdline,
                        "success_place_id": success_place_id,
                        "failure_place_id": failure_place_id,
                        }
                )

        self.assertEqual(good_cmdline, action.args["command_line"])

        orchestrator = mock.MagicMock()
        service_interfaces = {"orchestrator": orchestrator}
        net = mock.MagicMock()
        net.key = "netkey!"

        active_tokens_key = "x"
        action.execute(active_tokens_key, net, service_interfaces)
        orchestrator.set_token.assert_called_with(
                net.key, success_place_id, token_key=mock.ANY)

        action.args["command_line"] = fail_cmdline
        orchestrator.reset_mock()

        action.execute(active_tokens_key, net, service_interfaces)
        orchestrator.set_token.assert_called_with(
                    net.key, failure_place_id, token_key=mock.ANY)

    def test_required_arguments(self):
        self.assertRaises(TypeError, petri.SetRemoteTokenAction.create,
                connection=self.conn,
                name="TestAction",
                )

        self.assertRaises(TypeError, petri.SetRemoteTokenAction.create,
                connection=self.conn,
                name="TestAction",
                args={"remote_place_id": 1}
                )

        self.assertRaises(TypeError, petri.SetRemoteTokenAction.create,
                connection=self.conn,
                name="TestAction",
                args={"remote_net_key": "x"},
                )

    def test_set_remote_token_action(self):
        args = {"remote_place_id": 1, "remote_net_key": "netkey!",
                "data_type": "output"}

        action = petri.SetRemoteTokenAction.create(
                connection=self.conn,
                name="TestAction",
                args=args,
                )

        inputs = {"x": "y", "a": "b"}
        action.input_data = mock.Mock(return_value=inputs)

        orchestrator = mock.Mock()
        service_interfaces = {"orchestrator": orchestrator}
        net = mock.MagicMock()
        action.execute(active_tokens_key="x", net=net, service_interfaces=service_interfaces)
        orchestrator.set_token.assert_called_once_with("netkey!", 1, mock.ANY)

        token_key = orchestrator.set_token.call_args[0][2]
        token = petri.Token(connection=self.conn, key=token_key)
        self.assertEqual("output", token.data_type.value)
        self.assertEqual(inputs, token.data.value)


if __name__ == "__main__":
    unittest.main()
