from flow.petri_net.builder import future
from unittest import TestCase, main

from mock import Mock


class TestFutureNet(TestCase):
    def setUp(self):
        self.net = future.FutureNet('net')

    def test_init(self):
        self.assertEqual(self.net.name, 'net')

    def test_add_barrier_transition(self):
        action_class = Mock()
        action_args = Mock()

        trans = self.net.add_barrier_transition(name='t',
                action_class=action_class, action_args=action_args)

        self.assertItemsEqual([trans], self.net.transitions)
        self.assertIsInstance(trans, future.FutureBarrierTransition)
        self.assertIsInstance(trans.action, future.FutureAction)

    def test_add_basic_transition(self):
        action_class = Mock()
        action_args = Mock()

        trans = self.net.add_basic_transition(name='t',
                action_class=action_class, action_args=action_args)

        self.assertItemsEqual([trans], self.net.transitions)
        self.assertIsInstance(trans, future.FutureBasicTransition)
        self.assertIsInstance(trans.action, future.FutureAction)


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


    def get_unique_arc_out(self, source):
        return list(source.arcs_out)[0]

    def test_bridge_places(self):
        p1 = self.net.add_place()
        p2 = self.net.add_place()

        self.net.bridge_places(p1, p2)

        self.assertEqual(1, len(self.net.transitions))

        t = self.get_unique_arc_out(p1)
        self.assertIsInstance(t, future.FutureBasicTransition)

        final_p = self.get_unique_arc_out(t)
        self.assertIs(p2, final_p)

    def test_bridge_transitions(self):
        t1 = self.net.add_basic_transition()
        t2 = self.net.add_basic_transition()

        self.net.bridge_transitions(t1, t2)

        self.assertEqual(1, len(self.net.places))
        p = self.get_unique_arc_out(t1)
        self.assertIsInstance(p, future.FuturePlace)

        final_t = self.get_unique_arc_out(p)
        self.assertIs(t2, final_t)


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
