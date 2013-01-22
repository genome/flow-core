#!/usr/bin/env python

from flow.orchestrator import types as ft

from redistest import RedisTest
import mock
from mock import Mock
import unittest

class TestBase(unittest.TestCase):
    def setUp(self):
        self.conn = RedisTest()

    def _create_node(self, **kwargs):
        return ft.NodeBase.create(connection=self.conn, **kwargs)

class TestStatus(unittest.TestCase):
    def test_done(self):
        self.assertEqual(False, Status.done(Status.new))
        self.assertEqual(False, Status.done(Status.running))
        self.assertEqual(False, Status.done(Status.dispatched))
        self.assertEqual(True, Status.done(Status.success))
        self.assertEqual(True, Status.done(Status.failure))
        self.assertEqual(True, Status.done(Status.cancelled))

class TestNodeBase(TestBase):
    def test_construct_simple(self):
        node = self._create_node(
                flow_key="some_flow",
                name="Test Node",
                status=Status.new,
                )

        self.assertEqual("Test Node", node.name.value)
        self.assertEqual("some_flow", node.flow_key.value)
        self.assertEqual(Status.new, node.status.value)

    def test_duration(self):
        node = self._create_node(name="Test Node")
        self.assertEqual(None, node.duration)

        beg = node.execute_timestamp.setnx()
        self.assertTrue(float(beg) >= 0)
        self.assertTrue(node.duration >= 0)

        end = node.complete_timestamp.setnx()
        self.assertTrue(float(end) >= 0)
        self.assertEqual(float(end)-float(beg), node.duration)

    def test_flow_accessor(self):
        flow = ft.Flow.create(connection=self.conn, name="flow")
        node = self._create_node(name="Test Node")
        self.assertEqual(None, node.flow)
        node.flow_key = flow.key
        self.assertEqual(flow.key, node.flow.key)

    def test_outputs(self):
        outputs = {1: 2, 3: 4, "five": "six"}
        node = self._create_node(
                name="Test Node",
                outputs=outputs,
                )
        self.assertEqual(outputs, node.outputs.value)
        val = {"y": ["z", "zprime"]}
        node.outputs["x"] = val
        self.assertEqual(node.outputs["x"], val)

    def test_complete_sets_timestamp(self):
        node = self._create_node(name="Test Node")
        self.assertEqual(None, node.complete_timestamp.value)
        no_services = {}
        node.complete(no_services)
        when = node.complete_timestamp.value
        self.assertNotEqual(None, when)
        self.assertTrue(float(when) >= 0)
        self.assertRaises(ft.NodeAlreadyCompletedError, node.complete,
                          no_services)
        self.assertEqual(when, node.complete_timestamp.value)

    def test_complete_with_successors(self):
        flow = ft.Flow.create(connection=self.conn, name="Test Flow")
        nodes = [self._create_node(name="n%d" %x) for x in xrange(3)]

        for n in nodes:
            flow.add_node(n)

        # Successors are listed as indices into the flow.node_keys list
        nodes[0].successors.update([1, 2])
        nodes[1].indegree = 1
        nodes[2].indegree = 2

        mock_orchestrator = Mock()
        services = { "orchestrator": mock_orchestrator }

        nodes[0].complete(services)
        self.assertEqual(0, int(nodes[1].indegree))
        self.assertEqual(1, int(nodes[2].indegree))
        expected_call = mock.call.execute_node(nodes[1].key)
        mock_orchestrator.assert_has_calls(expected_call)
        self.assertEqual(1, len(mock_orchestrator.mock_calls))

        self.assertRaises(ft.NodeAlreadyCompletedError, nodes[0].complete,
                          services)

    def test_cancel(self):
        node = self._create_node(name="Test Node", status=Status.new)
        self.assertEqual(Status.new, node.status.value)
        no_services = {}
        node.cancel(no_services)
        self.assertEqual(Status.cancelled, node.status.value)

    def test_fail_with_no_successors(self):
        node = self._create_node(name="Test Node", status=Status.new)
        self.assertEqual(Status.new, node.status.value)
        no_services = {}
        node.fail(no_services)
        self.assertEqual(Status.failure, node.status.value)

    def test_fail_with_successors(self):
        flow = ft.Flow.create(connection=self.conn, name="Test Flow")
        nodes = [self._create_node(name="n%d" %x) for x in xrange(3)]

        for n in nodes:
            flow.add_node(n)

        # Successors are listed as indices into the flow.node_keys list
        nodes[0].successors.update([1, 2])
        nodes[1].indegree = 1
        nodes[2].indegree = 2

        # FIXME: Nodes should fail successors through the orchestrator.
        #        They currently do so by directly calling them.
        mock_orchestrator = Mock()
        services = { "orchestrator": mock_orchestrator }

        nodes[0].fail(services)
        self.assertEqual(Status.failure, nodes[0].status.value)
        self.assertEqual(Status.failure, nodes[1].status.value)
        self.assertEqual(Status.cancelled, nodes[2].status.value)

        self.assertEqual(0, int(nodes[1].indegree))
        self.assertEqual(1, int(nodes[2].indegree))

    def test_input_connections(self):
        node = self._create_node(name="Test Node")
        self.assertEqual({}, node.input_connections.value)
        self.assertEqual(None, node.inputs)

    def test_inherited_properties(self):
        f1_env = {"PATH": "/bin:/usr/bin", "HOME": "/home/frog"}
        f2_env = {"x": "y"}
        node_env = {"a": "b"}
        f1 = ft.Flow.create(connection=self.conn, name="f1")
        f2 = ft.Flow.create(connection=self.conn, name="f2")
        node = self._create_node(name="Test Node", flow_key=f2.key)
        f1.add_node(f2)
        f2.add_node(node)

        f1.environment = f1_env
        self.assertEqual(f1_env, node.environment.value)
        self.assertEqual(f1_env, f2.environment.value)
        self.assertEqual(f1_env, f1.environment.value)

        f2.environment = f2_env
        self.assertEqual(f2_env, node.environment.value)
        self.assertEqual(f2_env, f2.environment.value)
        self.assertEqual(f1_env, f1.environment.value)

        node.environment = node_env
        self.assertEqual(node_env, node.environment.value)
        self.assertEqual(f2_env, f2.environment.value)
        self.assertEqual(f1_env, f1.environment.value)

        del node.environment
        self.assertEqual(f2_env, node.environment.value)
        self.assertEqual(f2_env, f2.environment.value)
        self.assertEqual(f1_env, f1.environment.value)

        del f2.environment
        self.assertEqual(f1_env, node.environment.value)
        self.assertEqual(f1_env, f2.environment.value)
        self.assertEqual(f1_env, f1.environment.value)

        del f1.environment
        self.assertEqual(None, node.environment)
        self.assertEqual(None, f2.environment)
        self.assertEqual(None, f1.environment)

        f2.environment = f2_env
        self.assertEqual(f2_env, node.environment.value)
        self.assertEqual(f2_env, f2.environment.value)
        self.assertEqual(None, f1.environment)

        f1.user_id = '123'
        self.assertEqual('123', f1.user_id.value)
        self.assertEqual('123', f2.user_id.value)
        self.assertEqual('123', node.user_id.value)

        f1.working_directory = '/dir'
        self.assertEqual('/dir', f1.working_directory.value)
        self.assertEqual('/dir', f2.working_directory.value)
        self.assertEqual('/dir', node.working_directory.value)


class TestFlow(TestBase):
    def setUp(self):
        TestBase.setUp(self)
        self.flow = ft.Flow.create(connection=self.conn, name="Test flow")
        self.data = ft.DataNode.create(connection=self.conn, name="inputs")
        self.flow.input_connections[self.data.key] = {}
        self.native_outputs = {
                "scalar": "value",
                "hash": {"key": "value"},
                "list": [1, 2, 3],
                }
        self.data.outputs = self.native_outputs

    def test_add_node(self):
        self.assertEqual(0, len(self.flow.node_keys))
        node = self._create_node(name="Test Node")
        self.flow.add_node(node)
        self.assertEqual([node.key], self.flow.node_keys.value)
        self.assertEqual(self.flow.key, node.flow.key)

    def test_inputs(self):
        self.assertEqual(self.native_outputs, self.flow.inputs)

        start_node = ft.StartNode.create(connection=self.conn, name="start")
        self.flow.add_node(start_node)
        self.assertEqual(self.native_outputs, start_node.outputs)

        node = self._create_node(name="Test Node")
        self.flow.add_node(node)
        node.input_connections[start_node.key] = {
                "val": "scalar",
                "nums": "list"
                }
        self.assertEqual({"val": "value", "nums": [1, 2, 3]}, node.inputs)

    def test_stop_node_execute(self):
        # FIXME: stop nodes directly complete themselves and their flow,
        #        they should do it via the orchestrator

        stop_node = ft.StopNode.create(connection=self.conn, name="stop")
        self.flow.add_node(stop_node)
        self.assertEqual(self.flow.key, stop_node.flow.key)
        no_services = {}
        stop_node.execute(no_services)
        self.assertEqual(Status.success, stop_node.status.value)
        self.assertEqual(Status.success, self.flow.status.value)

    def test_stop_node_cancel(self):
        stop_node = ft.StopNode.create(connection=self.conn, name="stop")
        self.flow.add_node(stop_node)
        no_services = {}
        stop_node.cancel(no_services)
        self.assertEqual(Status.failure, stop_node.status.value)
        self.assertEqual(Status.failure, self.flow.status.value)

    def test_stop_node_fail(self):
        stop_node = ft.StopNode.create(connection=self.conn, name="stop")
        self.flow.add_node(stop_node)
        no_services = {}
        stop_node.fail(no_services)
        self.assertEqual(Status.failure, stop_node.status.value)
        self.assertEqual(Status.failure, self.flow.status.value)


if __name__ == "__main__":
    unittest.main()
