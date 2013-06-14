from flow.petri_net import success_failure_net
from net_helpers import get_unique_arc_in, get_unique_arc_out

import mock
import unittest

class SuccessFailureNetTest(unittest.TestCase):
    def setUp(self):
        self.net = success_failure_net.SuccessFailureNet()

    def test_start_places_connected(self):
        trans = get_unique_arc_out(self.net.start_place)
        self.assertEqual(self.net.start_transition, trans)

        internal_place = get_unique_arc_out(trans)
        self.assertEqual(self.net.internal_start_place, internal_place)

    def test_done_place_connected(self):
        external_trans = get_unique_arc_in(self.net.done_place)
        self.assertEqual(self.net.done_transition, external_trans)

        hidden_done_place = get_unique_arc_in(external_trans)

        hidden_failure_transition = get_unique_arc_out(
                self.net.internal_failure_place)
        hidden_success_transition = get_unique_arc_out(
                self.net.internal_failure_place)

        self.assertIn(hidden_done_place, hidden_failure_transition.arcs_out)
        self.assertIn(hidden_done_place, hidden_success_transition.arcs_out)

    def test_failure_places_connected(self):
        external_trans = get_unique_arc_in(self.net.failure_place)
        self.assertEqual(self.net.failure_transition, external_trans)

        hidden_place = get_unique_arc_in(external_trans)
        hidden_transition = get_unique_arc_in(hidden_place)
        internal_trans = get_unique_arc_out(self.net.internal_failure_place)
        self.assertEqual(internal_trans, hidden_transition)

    def test_success_places_connected(self):
        external_trans = get_unique_arc_in(self.net.success_place)
        self.assertEqual(self.net.success_transition, external_trans)

        hidden_place = get_unique_arc_in(external_trans)
        hidden_transition = get_unique_arc_in(hidden_place)
        internal_trans = get_unique_arc_out(self.net.internal_success_place)
        self.assertEqual(internal_trans, hidden_transition)


if __name__ == '__main__':
    unittest.main()
