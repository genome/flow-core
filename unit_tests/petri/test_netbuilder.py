import flow.petri.netbuilder as nb

import mock
import unittest
from itertools import combinations

class BuilderTest(unittest.TestCase):
    def setUp(self):
        self.builder = nb.NetBuilder()

class TestNodes(unittest.TestCase):
    def test_place(self):
        place = nb.Place(index=0, name="x")
        rep = str(place)
        self.assertTrue(rep.startswith("Place(index=0"))

    def test_transition(self):
        action = nb.ActionSpec(cls=None, args=None)
        trans = nb.Transition(index=0, name="x", action=action)
        rep = str(trans)
        self.assertTrue(rep.startswith("Transition(index=0"))
        self.assertTrue("ActionSpec" in rep)

class TestNetBuilder(BuilderTest):
    def test_graph(self):
        net = self.builder.add_subnet(nb.EmptyNet, "hi")
        start = net.add_place("start")
        p1 = net.add_place("p1")
        p2 = net.add_place("p2")
        end = net.add_place("end")

        t1 = net.add_transition(name="t1")
        t2 = net.add_transition(name="t2")

        start.arcs_out.add(t1)
        p1.arcs_out.add(t2)
        p2.arcs_out.add(t1)
        t1.arcs_out.add(p1)
        t1.arcs_out.add(p2)
        t2.arcs_out.add(end)

        expected_place_names = ["start", "p1", "p2", "end"]
        place_names = [x.name for x in self.builder.places]
        self.assertEqual(expected_place_names, place_names)
        self.assertEqual(["t1", "t2"], [x.name for x in net.transitions])

        graph = self.builder.graph()
        self.assertEqual(6, len(graph.nodes()))
        self.assertEqual(6, len(graph.edges()))

        expected_node_labels = sorted(expected_place_names + ["t1", "t2"])
        node_labels = sorted([x.attr["label"] for x in graph.nodes()])
        self.assertEqual(expected_node_labels, node_labels)

        # Make sure the graph is bipartite. Place and transition nodes in the
        # graphviz graph are # always labeled p0, ..., pN and t0, ..., tN,
        # respectively. We happened to use the same names when constructing
        # our net.
        for edge in graph.edges():
            # all edges should be between some "p_x" and "t_x"
            nodes = [graph.get_node(x) for x in edge]
            node_types = sorted([x.attr["_type"] for x in nodes])
            self.assertEqual(["Place", "Transition"], node_types)

    def test_graph_with_subgraphs(self):
        net0 = self.builder.add_subnet(nb.EmptyNet, "net0")
        p0 = net0.add_place("p0")

        # net1 is a subnet of net0
        net1 = net0.add_subnet(nb.EmptyNet, "net1")
        p1 = net1.add_place("p1")

        # net2 is a subnet of net1
        net2 = net1.add_subnet(nb.EmptyNet, "net2")
        p2 = net2.add_place("p2")

        # net3 is a subnet of net0
        net3 = net0.add_subnet(nb.EmptyNet, "net3")
        p3 = net3.add_place("p3")

        net0.bridge_places(p0, p1, "t0")
        net1.bridge_places(p1, p2, "t1")
        net2.bridge_places(p2, p3, "t2")

        graph = self.builder.graph(subnets=True)
        graph.draw("/dev/null", prog="dot")

        nodes = graph.nodes()
        edges = graph.edges()

        self.assertEqual(7, len(nodes))
        self.assertEqual(6, len(edges))

        subgraphs = [None]*4

        # the outer subgraph (net0)
        (subgraphs[0],) = graph.subgraphs()

        # net0 has 2 subgraphs: net1 and net3
        # note that the order we get them back in is arbitrary
        tmp = {x.name: x for x in subgraphs[0].subgraphs()}
        self.assertEqual(set(["cluster_1", "cluster_3"]), set(tmp.keys()))
        subgraphs[1] = tmp["cluster_1"]
        subgraphs[3] = tmp["cluster_3"]

        # net1 has 1 subgraph: net2
        (subgraphs[2],) = subgraphs[1].subgraphs()

        nodes = []
        node_labels = []

        for i in xrange(4):
            self.assertEqual("cluster_%d" % i , subgraphs[i].name)
            self.assertEqual("net%d" % i , subgraphs[i].graph_attr["label"])
            nodes.append(subgraphs[i].nodes())
            node_labels.append(sorted(x.attr["label"] for x in nodes[i]))

        self.assertEqual(7, len(nodes[0]))
        self.assertEqual(4, len(nodes[1]))
        self.assertEqual(2, len(nodes[2]))
        self.assertEqual(1, len(nodes[3]))

        expected_node_labels = [
            ["p0", "p1", "p2", "p3", "t0", "t1", "t2"], # net 0 has all nodes
            ["p1", "p2", "t1", "t2"], # net 1 has p1/2, t1/2
            ["p2", "t2"], # net 2 has p2, t2
            ["p3"], # net 3 has p3
        ]

        self.assertEqual(expected_node_labels, node_labels)

    def test_bridge_places_with_action(self):
        net = self.builder.add_subnet(nb.EmptyNet, "test")
        p1 = net.add_place("p1")
        p2 = net.add_place("p2")
        action = mock.Mock()
        t = net.bridge_places(p1, p2, action=action)
        self.assertEqual(action, t.action)

    def test_success_failure_net(self):
        net = self.builder.add_subnet(nb.SuccessFailureNet, "sfnet")
        expected_places = ["start", "success", "failure"]
        for place_name in expected_places:
            place = getattr(net, place_name)
            self.assertTrue(isinstance(place, nb.Place))

        self.assertEqual(len(expected_places), len(net.places))
        self.assertEqual([], net.transitions)

    def test_shell_command_net(self):
        net = self.builder.add_subnet(nb.ShellCommandNet, "scnet", ["ls", "-al"])

        expected_places = ["start", "success", "failure", "on_success_place",
                "on_failure_place", "running"]

        for place_name in expected_places:
            place = getattr(net, place_name)
            self.assertTrue(isinstance(place, nb.Place))

        self.assertEqual(len(expected_places), len(net.places))
        self.assertEqual(3, len(net.transitions))

    def test_bridge(self):
        p1 = self.builder.add_place("p1")
        p2 = self.builder.add_place("p2")
        t1 = self.builder.add_transition()
        t2 = self.builder.add_transition()

        self.assertRaises(TypeError, self.builder.bridge_places, p1, "p2")
        self.assertRaises(TypeError, self.builder.bridge_places, p1, t1)
        self.assertRaises(TypeError, self.builder.bridge_places, t1, p1)

        self.assertRaises(TypeError, self.builder.bridge_transitions, t1, "t2")
        self.assertRaises(TypeError, self.builder.bridge_transitions, t1, p1)
        self.assertRaises(TypeError, self.builder.bridge_transitions, p1, t1)

        sneakp = nb.Place(3, "sneak")
        sneakt = nb.Transition(3, "sneak")
        self.assertRaises(RuntimeError, self.builder.bridge_places, p1, sneakp)
        self.assertRaises(RuntimeError, self.builder.bridge_transitions, t1, sneakt)

        tmp = self.builder.bridge_places(p1, p2)
        self.assertIsInstance(tmp, nb.Transition)
        self.assertEqual(set([tmp]), p1.arcs_out)
        self.assertEqual(set([p2]), tmp.arcs_out)

        tmp = self.builder.bridge_transitions(t1, t2)
        self.assertIsInstance(tmp, nb.Place)
        self.assertEqual(set([tmp]), t1.arcs_out)
        self.assertEqual(set([t2]), tmp.arcs_out)

class TestEmptyNet(BuilderTest):
    def test_names_auto_increment(self):
        net = self.builder.add_subnet(nb.EmptyNet, "test")

        transitions = [net.add_transition() for x in range(3)]
        names = [t.name for t in transitions]
        self.assertEqual(["t0", "t1", "t2"], names)

        places = [net.add_place() for x in range(3)]
        names = [p.name for p in places]
        self.assertEqual(["p0", "p1", "p2"], names)

    def test_bridge(self):
        net1 = self.builder.add_subnet(nb.EmptyNet, "test")
        p1 = net1.add_place("p1")
        p2 = net1.add_place("p2")
        place_bridge = net1.bridge_places(p1, p2, "pb")

        net2 = self.builder.add_subnet(nb.EmptyNet, "test")
        t1 = net2.add_transition("t1")
        t2 = net2.add_transition("t2")
        trans_bridge = net2.bridge_transitions(t1, t2, "tb")

        # Build a bridge from p2 in net1 to a new place p3 in net2
        # Since we're asking net1 to do it, the resulting transition will live
        # in net1.
        p3 = net2.add_place("p3")
        cross_net_bridge = net1.bridge_places(p2, p3, "cross net")

        self.assertEqual("pb", place_bridge.name)
        self.assertEqual("tb", trans_bridge.name)
        self.assertEqual("cross net", cross_net_bridge.name)

        # Builder should see all bridges
        self.assertIn(place_bridge, self.builder._trans_map)
        self.assertIn(trans_bridge, self.builder._place_map)
        self.assertIn(cross_net_bridge, self.builder._trans_map)

        # make sure bridge nodes are owned by the net that created them
        self.assertIn(place_bridge, net1._trans_set)
        self.assertNotIn(place_bridge, net2._trans_set)
        self.assertIn(cross_net_bridge, net1._trans_set)
        self.assertNotIn(cross_net_bridge, net2._trans_set)

        self.assertIn(trans_bridge, net2._place_set)
        self.assertNotIn(trans_bridge, net1._place_set)

        self.assertIsInstance(trans_bridge, nb.Place)
        self.assertIsInstance(place_bridge, nb.Transition)
        self.assertIsInstance(cross_net_bridge, nb.Transition)

        self.assertEqual(set([place_bridge]), p1.arcs_out)
        self.assertEqual(set([p2]), place_bridge.arcs_out)
        self.assertEqual(set([cross_net_bridge]), p2.arcs_out)
        self.assertEqual(set([p3]), cross_net_bridge.arcs_out)

        self.assertEqual(set([trans_bridge]), t1.arcs_out)
        self.assertEqual(set([t2]), trans_bridge.arcs_out)



if __name__ == "__main__":
    unittest.main()
