#!/usr/bin/env python

import flow.orchestrator.types as flnodes

import unittest
from fakeredis import FakeRedis

class TestBase(unittest.TestCase):
    def setUp(self):
        self.conn = FakeRedis()

class TestNodeBase(TestBase):
    def test_construct_simple(self):
        node = flnodes.NodeBase.create(
                connection=self.conn,
                flow_key="some_flow",
                name="Test Node",
                status=flnodes.Status.new,
                )

        self.assertEqual("Test Node", node.name.value)
        self.assertEqual("some_flow", node.flow_key.value)
        self.assertEqual(flnodes.Status.new, node.status.value)

    def test_outputs(self):
        outputs = {1: 2, 3: 4, "five": "six"}
        node = flnodes.NodeBase.create(
                connection=self.conn,
                name="Test Node",
                outputs=outputs,
                )
        self.assertEqual(outputs, node.outputs.value)
        val = {"y": ["z", "zprime"]}
        node.outputs["x"] = val
        self.assertEqual(node.outputs["x"], val)

if __name__ == "__main__":
    unittest.main()
