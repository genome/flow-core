import flow.petri.netbuilder as nb

import unittest
from itertools import combinations

class TestNet(unittest.TestCase):
    def test_construct_graph(self):
        net = nb.Net("net")

        places = []
        for x in xrange(4):
            places.append(net.add_place(nb.Place("p%d" % x)))

        t0 = net.add_transition(nb.Transition(name="t0"))
        t1 = net.add_transition(nb.Transition(name="t1"))

        net.add_place_arc_out(places[0], t0)
        net.add_trans_arc_out(t0, places[1])
        net.add_trans_arc_out(t0, places[2])
        net.add_place_arc_out(places[1], t1)
        net.add_place_arc_out(places[2], t1)
        net.add_trans_arc_out(t1, places[3])

        place_names = [x.name for x in net.places]
        self.assertEqual(["p%d" % x for x in xrange(4)], place_names)
        self.assertEqual(["t0", "t1"], [x.name for x in net.transitions])
        expected_trans_arcs_out = {
            t0: set([places[1], places[2]]),
            t1: set([places[3]]),
        }
        expected_place_arcs_out = {
            places[0]: set([t0]),
            places[1]: set([t1]),
            places[2]: set([t1])
        }
        self.assertEqual(expected_place_arcs_out, net.place_arcs_out)
        self.assertEqual(expected_trans_arcs_out, net.trans_arcs_out)

        graph = net.graph()
        self.assertEqual(6, len(graph.nodes()))
        self.assertEqual(6, len(graph.edges()))

        expected_node_labels = ["p%d" % x for x in xrange(4)] + ["t0", "t1"]
        node_labels = [x.attr["label"] for x in graph.nodes()]
        self.assertEqual(expected_node_labels, node_labels)

        # Make sure the graph is bipartite. Place and transition nodes in the
        # graphviz graph are # always labeled p0, ..., pN and t0, ..., tN,
        # respectively. We happened to use the same names when constructing
        # our net.
        for edge in graph.edges():
            # all edges should be between some "p_x" and "t_x"
            edge = list(sorted(edge))
            self.assertEqual("p", edge[0][0])
            self.assertEqual("t", edge[1][0])

    def test_update(self):
        netA = nb.Net("netA")
        netB = nb.Net("netB")

        pA0 = netA.add_place(nb.Place("pA0"))
        pA1 = netA.add_place(nb.Place("pA1"))

        pB0 = netB.add_place(nb.Place("pB0"))
        pB1 = netB.add_place(nb.Place("pB1"))

        tA0 = netA.add_transition(nb.Transition(name="tA0", place_refs=[pA0]))
        tB0 = netB.add_transition(nb.Transition(name="tB0", place_refs=[pB0]))

        netA.add_place_arc_out(pA0, tA0)
        netA.add_trans_arc_out(tA0, pA1)

        netB.add_place_arc_out(pB0, tB0)
        netB.add_trans_arc_out(tB0, pB1)

        netA.update(netB, {pA1: pB0})

        # make sure netB is unchanged
        self.assertEqual(["pB0", "pB1"], [x.name for x in netB.places])
        self.assertEqual(["tB0"], [x.name for x in netB.transitions])
        self.assertEqual({pB0: set([tB0])}, netB.place_arcs_out)
        self.assertEqual({tB0: set([pB1])}, netB.trans_arcs_out)

        place_names = [x.name for x in netA.places]
        self.assertEqual(["pA0", "pA1", "pB0", "pB1"], place_names)
        trans_names = set([x.name for x in netA.transitions])
        self.assertTrue("tA0" in trans_names)
        self.assertTrue("tB0" in trans_names)

        tA0_ref = netA.transitions[0].place_refs[0]
        tB0_ref = netA.transitions[1].place_refs[0]
        self.assertEqual("pA0", netA.places[tA0_ref].name)
        self.assertEqual("pB0", netA.places[tB0_ref].name)


if __name__ == "__main__":
    unittest.main()
