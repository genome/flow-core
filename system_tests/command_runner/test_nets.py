import flow.command_runner.executors.nets as enets
import flow.petri.netbuilder as nb

import test_helpers
import unittest

class TestLSFDispatchAction(test_helpers.RedisTest):
    def test_response_places(self):
        cmdline = ["ls", "-al"]
        builder = nb.NetBuilder("test")
        net = enets.LSFCommandNet(builder, name="test",
                action_class=enets.LSFDispatchAction,
                action_args={"command_line": cmdline})

        expected = {
            'post_dispatch_success': str(net.dispatch_success_place.index),
            'post_dispatch_failure': str(net.dispatch_failure_place.index),
            'begin_execute': str(net.begin_execute_place.index),
            'execute_success': str(net.execute_success_place.index),
            'execute_failure': str(net.execute_failure_place.index),
        }

        stored_net = builder.store(self.conn)
        dispatch_transition = stored_net.transition(0)
        self.assertEqual("dispatch", str(dispatch_transition.name))
        action = dispatch_transition.action
        self.assertIsInstance(action, enets.LSFDispatchAction)

        response_places = action._response_places()
        self.assertEqual(expected, response_places)


class TestLocalDispatchAction(test_helpers.RedisTest):
    def test_response_places(self):
        cmdline = ["ls", "-al"]
        builder = nb.NetBuilder("test")
        net = enets.LocalCommandNet(builder, name="test",
                action_class=enets.LocalDispatchAction,
                action_args={"command_line": cmdline})

        expected = {
            'pre_dispatch': str(net.on_begin_execute.index),
            'post_dispatch_success': str(net.on_execute_success.index),
            'post_dispatch_failure': str(net.on_execute_failure.index),
        }

        stored_net = builder.store(self.conn)
        dispatch_transition = stored_net.transition(0)
        self.assertEqual("dispatch", str(dispatch_transition.name))
        action = dispatch_transition.action
        self.assertIsInstance(action, enets.LocalDispatchAction)

        response_places = action._response_places()
        self.assertEqual(expected, response_places)



if __name__ == "__main__":
    unittest.main()
