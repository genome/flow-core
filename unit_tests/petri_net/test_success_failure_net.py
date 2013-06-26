from flow.petri_net import success_failure_net
from net_helpers import get_unique_arc_in, get_unique_arc_out

import mock
import unittest
from flow.petri_net.future import FutureBasicTransition

class SuccessFailureNetTest(unittest.TestCase):
    def setUp(self):
        self.net = success_failure_net.SuccessFailureNet()

    def test_external_api(self):
        self.assertIsInstance(self.net.start_transition,
                FutureBasicTransition)
        self.assertIsInstance(self.net.success_transition,
                FutureBasicTransition)
        self.assertIsInstance(self.net.failure_transition,
                FutureBasicTransition)

    def test_internal_api(self):
        self.assertIsInstance(self.net.internal_start_transition,
                FutureBasicTransition)
        self.assertIsInstance(self.net.internal_success_transition,
                FutureBasicTransition)
        self.assertIsInstance(self.net.internal_failure_transition,
                FutureBasicTransition)

    def test_wrap_with_places(self):
        self.net.wrap_with_places()

        self.assertIn(self.net.start_transition,
                self.net.start_place.arcs_out)
        self.assertIn(self.net.success_transition,
                self.net.success_place.arcs_in)
        self.assertIn(self.net.failure_transition,
                self.net.failure_place.arcs_in)

if __name__ == '__main__':
    unittest.main()
