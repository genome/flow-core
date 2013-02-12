import flow.petri.netbuilder as nb

import unittest
from itertools import combinations

class TestNetBuilder(unittest.TestCase):
    def setUp(self):
        self.builder = nb.NetBuilder("test")

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
        net1 = self.builder.add_subnet(nb.EmptyNet, "net1")
        p1 = net1.add_place("p1")

        net2 = self.builder.add_subnet(nb.EmptyNet, "net2")
        p2 = net2.add_place("p2")

        self.builder.bridge_places(p1, p2)
        graph = self.builder.graph(subnets=True)

        nodes = graph.nodes()
        edges = graph.edges()
        subgraphs = graph.subgraphs()

        self.assertEqual(3, len(nodes))
        self.assertEqual(2, len(edges))
        self.assertEqual(2, len(subgraphs))

        nodes1 = subgraphs[0].nodes()
        nodes2 = subgraphs[1].nodes()

        self.assertEqual(1, len(nodes1))
        self.assertEqual(1, len(nodes2))
        both = sorted([nodes1[0].attr["label"], nodes2[0].attr["label"]])
        self.assertEqual(["p1", "p2"], both)

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

class TestEmptyNet(unittest.TestCase):
    def test_names_auto_increment(self):
        builder = nb.NetBuilder("test")
        net = builder.add_subnet(nb.EmptyNet, "test")

        transitions = [net.add_transition() for x in range(3)]
        names = [t.name for t in transitions]
        self.assertEqual(["t0", "t1", "t2"], names)

        places = [net.add_place() for x in range(3)]
        names = [p.name for p in places]
        self.assertEqual(["p0", "p1", "p2"], names)


if __name__ == "__main__":
    unittest.main()
