from flow.shell_command.petri_net import actions
from flow.shell_command.petri_net import future_nets

import mock
import unittest


class FakeShellCommandNet(future_nets.ShellCommandNet):
    def __init__(self, *args, **kwargs):
        future_nets.ShellCommandNet.__init__(self,
                *args, **kwargs)


class ShellCommandNetTest(unittest.TestCase):
    def setUp(self):
        self.dispatch_action_class = mock.Mock()
        self.arg1 = mock.Mock()
        self.arg2 = mock.Mock()

        self.net = FakeShellCommandNet(name='foo',
                dispatch_action_class=self.dispatch_action_class,
                arg1=self.arg1, arg2=self.arg2)
        self.net.wrap_with_places()

    def test_unreachable_places(self):
        unreachable_places = {p for p in self.net.places if not p.arcs_in}

        expected_unreachable_places = {
            self.net.start_place,
            self.net.dispatch_failure_place,
            self.net.dispatch_success_place,
            self.net.execute_begin_place,
            self.net.execute_failure_place,
            self.net.execute_success_place,
        }
        self.assertEqual(expected_unreachable_places, unreachable_places)

    def test_dead_end_places(self):
        dead_end_places = {p for p in self.net.places if not p.arcs_out}

        expected_dead_end_places = {
            self.net.failure_place,
            self.net.success_place,
        }
        self.assertEqual(expected_dead_end_places, dead_end_places)


    def test_action_set(self):
        self.assertEqual(self.dispatch_action_class,
                self.net.dispatch_transition.action.cls)
        expected_args = {
            'arg1': self.arg1,
            'arg2': self.arg2,
            "msg: dispatch_success": self.net.dispatch_success_place,
            "msg: dispatch_failure": self.net.dispatch_failure_place,
            "msg: execute_begin": self.net.execute_begin_place,
            "msg: execute_success": self.net.execute_success_place,
            "msg: execute_failure": self.net.execute_failure_place,
        }

        self.assertEqual(expected_args,
                self.net.dispatch_transition.action.args)


    def test_dispatch_path(self):
        self.assertIn(self.net.dispatch_transition,
                self.net.internal_start_place.arcs_out)
        self.assertIn(self.net.dispatching_place,
                self.net.dispatch_transition.arcs_out)
        self.assertIn(self.net.dispatch_success_transition,
                self.net.dispatching_place.arcs_out)
        self.assertIn(self.net.pending_place,
                self.net.dispatch_success_transition.arcs_out)

    def test_running_path(self):
        self.assertIn(self.net.execute_begin_transition,
                self.net.pending_place.arcs_out)
        self.assertIn(self.net.running_place,
                self.net.execute_begin_transition.arcs_out)

    def test_execute_succes_path(self):
        self.assertIn(self.net.execute_success_transition,
                self.net.running_place.arcs_out)
        self.assertIn(self.net.success_cleanup_place,
                self.net.execute_success_transition.arcs_out)

    def test_path_to_failure_from_dispatch(self):
        self.assertIn(self.net.dispatch_failure_transition,
                self.net.dispatching_place.arcs_out)
        self.assertIn(self.net.failure_cleanup_place,
                self.net.dispatch_failure_transition.arcs_out)

    def test_path_to_failure_from_execute(self):
        self.assertIn(self.net.execute_failure_transition,
                self.net.running_place.arcs_out)
        self.assertIn(self.net.failure_cleanup_place,
                self.net.execute_failure_transition.arcs_out)

    def test_success_cleanup_path(self):
        self.assertIn(self.net.success_cleanup_transition,
                self.net.success_cleanup_place.arcs_out)
        self.assertIn(self.net.internal_success_place,
                self.net.success_cleanup_transition.arcs_out)
        self.assertIn(self.net.internal_success_transition,
                self.net.internal_success_place.arcs_out)

    def test_failure_cleanup_path(self):
        self.assertIn(self.net.failure_cleanup_place,
                self.net.execute_failure_transition.arcs_out)
        self.assertIn(self.net.failure_cleanup_transition,
                self.net.failure_cleanup_place.arcs_out)
        self.assertIn(self.net.internal_failure_place,
                self.net.failure_cleanup_transition.arcs_out)
        self.assertIn(self.net.internal_failure_transition,
                self.net.internal_failure_place.arcs_out)


if __name__ == "__main__":
    unittest.main()
