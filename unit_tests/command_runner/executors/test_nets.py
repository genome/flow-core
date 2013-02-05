import flow.command_runner.executors.nets as enets
import flow.petri.netbuilder as nb

import unittest


class TestLSFCommandNet(unittest.TestCase):
    def test_construct(self):
        builder = nb.NetBuilder("test")
        cmdline = ["ls", "-al"]
        net = enets.LSFCommandNet(builder, "test lsf", cmdline)
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
        self.assertEqual(enets.LSFDispatchAction, net.dispatch.action_class)
        expected_args = {"command_line": cmdline}
        self.assertEqual(expected_args, net.dispatch.action_args)

        expected_place_refs = [
                net.dispatch_success_place.index,
                net.dispatch_failure_place.index,
                net.begin_execute_place.index,
                net.execute_success_place.index,
                net.execute_failure_place.index,
        ]
        self.assertEqual(expected_place_refs, net.dispatch.place_refs)


if __name__ == "__main__":
    unittest.main()
