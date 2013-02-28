import flow.command_runner.executors.nets as enets
import flow.petri.netbuilder as nb

import unittest


class TestLSFCommandNet(unittest.TestCase):
    def test_construct(self):
        builder = nb.NetBuilder()
        cmdline = ["ls", "-al"]

        net = enets.LSFCommandNet(builder, "test lsf",
                action_class=enets.LSFDispatchAction,
                action_args={"command_line": cmdline})

        expected_places = ["start", "success", "failure", "dispatching",
            "dispatch_success_place", "dispatch_failure_place",
            "pending", "begin_execute_place", "running",
            "execute_success_place", "execute_failure_place",
            ]

        for place_name in expected_places:
            place = getattr(net, place_name, None)
            self.assertTrue(isinstance(place, nb.Place))

        self.assertEqual(len(expected_places), len(net.places))

        self.assertTrue(isinstance(net.dispatch, nb.Transition))

        action = net.dispatch.action

        expected_args = {
                "command_line": cmdline,
                "post_dispatch_success": net.dispatch_success_place.index,
                "post_dispatch_failure": net.dispatch_failure_place.index,
                "begin_execute": net.begin_execute_place.index,
                "execute_success": net.execute_success_place.index,
                "execute_failure": net.execute_failure_place.index,
                }

        self.assertIsInstance(action, nb.ActionSpec)
        self.assertEqual(enets.LSFDispatchAction, action.cls)
        self.assertEqual(expected_args, action.args)


class TestLocalCommandNet(unittest.TestCase):
    def test_construct(self):
        builder = nb.NetBuilder()
        cmdline = ["ls", "-al"]

        net = enets.LocalCommandNet(builder, "test lsf",
                action_class=enets.LocalDispatchAction,
                action_args={"command_line": cmdline})

        expected_places = ["start", "success", "failure", "dispatched",
                "running", "on_begin_execute", "on_execute_success",
                "on_execute_failure"]

        expected_transitions = ["dispatch", "t_begin_execute",
                "execute_success", "execute_failure"]

        for place_name in expected_places:
            place = getattr(net, place_name, None)
            self.assertTrue(isinstance(place, nb.Place))
        self.assertEqual(len(expected_places), len(net.places))

        for transition_name in expected_transitions:
            transition = getattr(net, transition_name, None)
            self.assertTrue(isinstance(transition, nb.Transition))
        self.assertEqual(len(expected_transitions), len(net.transitions))

        self.assertEqual(set([net.dispatch]), net.start.arcs_out)
        self.assertEqual(set([net.dispatched]), net.dispatch.arcs_out)
        self.assertEqual(set([net.t_begin_execute]), net.dispatched.arcs_out)
        self.assertEqual(set([net.t_begin_execute]),
                net.on_begin_execute.arcs_out)

        self.assertEqual(set([net.running]), net.t_begin_execute.arcs_out)
        self.assertEqual(set([net.execute_success, net.execute_failure]),
                net.running.arcs_out)

        self.assertEqual(set([net.execute_success]),
                net.on_execute_success.arcs_out)

        self.assertEqual(set([net.execute_failure]),
                net.on_execute_failure.arcs_out)

        action = net.dispatch.action

        expected_args = {
                "command_line": cmdline,
                "begin_execute": net.on_begin_execute.index,
                "execute_success": net.on_execute_success.index,
                "execute_failure": net.on_execute_failure.index,
                }

        self.assertIsInstance(action, nb.ActionSpec)
        self.assertEqual(enets.LocalDispatchAction, action.cls)
        self.assertEqual(expected_args, action.args)


if __name__ == "__main__":
    unittest.main()
