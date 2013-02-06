import flow.petri.netbuilder as nb

import unittest
from itertools import combinations

class TestNetBuilder(unittest.TestCase):
    def test_construct_graph(self):
        builder = nb.NetBuilder("net")

        net = nb.EmptyNet(builder, "hi")
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
        place_names = [x.name for x in builder.places]
        self.assertEqual(expected_place_names, place_names)
        self.assertEqual(["t1", "t2"], [x.name for x in net.transitions])

        graph = builder.graph()
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

    def test_success_failure_net(self):
        builder = nb.NetBuilder("test")
        net = nb.SuccessFailureNet(builder, "sfnet")
        expected_places = ["start", "success", "failure"]
        for place_name in expected_places:
            place = getattr(net, place_name)
            self.assertTrue(isinstance(place, nb.Place))

        self.assertEqual(len(expected_places), len(net.places))
        self.assertEqual([], net.transitions)

    def test_shell_command_net(self):
        builder = nb.NetBuilder("test")
        net = nb.ShellCommandNet(builder, "scnet", ["ls", "-al"])

        expected_places = ["start", "success", "failure", "on_success_place",
                "on_failure_place", "running"]

        for place_name in expected_places:
            place = getattr(net, place_name)
            self.assertTrue(isinstance(place, nb.Place))

        self.assertEqual(len(expected_places), len(net.places))
        self.assertEqual(3, len(net.transitions))

    # TODO: implement and test that trying to bridge places unknown by the
    # NetBuilder instance raises an exception
    def test_bridge_places(self):
        builder = nb.NetBuilder("test")
        netA = nb.SuccessFailureNet(builder, name="netA")
        netB = nb.SuccessFailureNet(builder, name="netB")

        self.assertEqual(len(builder.places),
                len(netA.places) + len(netB.places))

        self.assertEqual([], builder.transitions)

        builder.bridge_places(netA.failure, netB.start)
        self.assertEqual(len(builder.places),
                len(netA.places) + len(netB.places))

        self.assertEqual(1, len(builder.transitions))
        bridge = builder.transitions[0]
        self.assertEqual(bridge, iter(netA.failure.arcs_out).next())
        self.assertEqual(netB.start, iter(bridge.arcs_out).next())



if __name__ == "__main__":
    unittest.main()
