from flow.petri_net import future
from net_helpers import get_unique_arc_in, get_unique_arc_out
from unittest import TestCase, main

from mock import Mock


class TestFutureNet(TestCase):
    def setUp(self):
        self.net = future.FutureNet('net')

    def test_init(self):
        self.assertEqual(self.net.name, 'net')

    def test_add_barrier_transition(self):
        trans = self.net.add_barrier_transition(name='t')

        self.assertItemsEqual([trans], self.net.transitions)
        self.assertIsInstance(trans, future.FutureBarrierTransition)

    def test_add_basic_transition(self):
        trans = self.net.add_basic_transition(name='t')

        self.assertItemsEqual([trans], self.net.transitions)
        self.assertIsInstance(trans, future.FutureBasicTransition)


    def test_add_place(self):
        place = self.net.add_place('p')
        self.assertItemsEqual([place], self.net.places)
        self.assertIsInstance(place, future.FuturePlace)

    def test_add_subnet(self):
        subnet_class = Mock()
        expected_subnet = Mock()
        subnet_class.return_value = expected_subnet
        kwargs = {
            'name': 's',
            'arg': 'foo',
        }

        subnet = self.net.add_subnet(subnet_class, **kwargs)

        self.assertEqual(expected_subnet, subnet)
        self.assertItemsEqual([subnet], self.net.subnets)
        subnet_class.assert_called_once_with(**kwargs)


    def test_bridge_places(self):
        p1 = self.net.add_place()
        p2 = self.net.add_place()

        self.net.bridge_places(p1, p2)

        self.assertEqual(1, len(self.net.transitions))

        t = get_unique_arc_out(p1)
        self.assertIsInstance(t, future.FutureBasicTransition)

        final_p = get_unique_arc_out(t)
        self.assertIs(p2, final_p)

    def test_bridge_transitions(self):
        t1 = self.net.add_basic_transition()
        t2 = self.net.add_basic_transition()

        self.net.bridge_transitions(t1, t2)

        self.assertEqual(1, len(self.net.places))
        p = get_unique_arc_out(t1)
        self.assertIsInstance(p, future.FuturePlace)

        final_t = get_unique_arc_out(p)
        self.assertIs(t2, final_t)

    def test_observe_transition(self):
        t = self.net.add_basic_transition()
        a = future.FutureAction(cls=Mock(), args=Mock())

        self.net.observe_transition(t, a)
        self.assertEqual(1, len(self.net.places))
        self.assertEqual(2, len(self.net.transitions))
        place = get_unique_arc_out(t)
        observer = get_unique_arc_out(place)
        self.assertEqual(a, observer.action)

    def test_split_place(self):
        num_out_places = 3

        p_in = self.net.add_place()

        out_places = [self.net.add_place() for x in xrange(num_out_places)]

        self.net.split_place(p_in, out_places)

        self.assertEqual(num_out_places + 1, len(self.net.places))
        self.assertEqual(1, len(self.net.transitions))

        for op in out_places:
            t = get_unique_arc_in(op)
            self.assertEqual(p_in, get_unique_arc_in(t))

    def test_join_transitions_as_or(self):
        num_sources = 3

        destination = self.net.add_basic_transition()
        sources = [self.net.add_basic_transition() for x in xrange(num_sources)]

        place = self.net.join_transitions_as_or(destination=destination,
                sources=sources, name='test_name')

        self.assertEqual(num_sources + 1, len(self.net.transitions))
        self.assertEqual(1, len(self.net.places))
        self.assertEqual(place.name, 'test_name')

        for transition in sources:
            p = get_unique_arc_out(transition)
            self.assertEqual(destination, get_unique_arc_out(p))


class TestFutureNode(TestCase):
    def setUp(self):
        self.node = future.FutureNode('tweedle dee')
        self.other = future.FutureNode('tweedle dum')

    def test_init(self):
        self.assertEqual(self.node.name, 'tweedle dee')
        self.assertEqual(0, len(self.node.arcs_in))
        self.assertEqual(0, len(self.node.arcs_out))

    def test_add_arc_in(self):
        self.node.add_arc_in(self.other)

        self.assertItemsEqual([self.other], self.node.arcs_in)
        self.assertItemsEqual([self.node], self.other.arcs_out)

    def test_add_arc_out(self):
        self.node.add_arc_out(self.other)

        self.assertItemsEqual([self.other], self.node.arcs_out)
        self.assertItemsEqual([self.node], self.other.arcs_in)


if __name__ == "__main__":
    main()
