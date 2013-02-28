import flow.command_runner.executors.nets as enets
import flow.petri.netbuilder as nb

import os
import test_helpers
import unittest

class TestBase(test_helpers.RedisTest):
    def setUp(self):
        test_helpers.RedisTest.setUp(self)

        self.cmdline = ["ls", "-al"]
        self.builder = nb.NetBuilder()
        self.net = self.net_class(self.builder, name="test",
                action_class=self.action_class,
                action_args={"command_line": self.cmdline})
        self.stored_net = self.builder.store(self.conn)
        self.dispatch_transition = self.stored_net.transition(0)
        self.action = self.dispatch_transition.action


class _TestDispatchActionMixIn(object):
    def test_command_line(self):
        self.assertEqual(self.cmdline, self.action._command_line(net=None,
                input_data_key=None))

    def test_executor_options(self):
        executor_options = self.action._executor_options(input_data_key=None,
                net=self.stored_net)

        self.assertEqual({}, executor_options)
        env = os.environ.data
        self.stored_net.set_constant("environment", env)

        expected = {"environment": env}
        executor_options = self.action._executor_options(input_data_key=None,
                net=self.stored_net)
        self.assertEqual(expected, executor_options)

        self.stored_net.set_constant("user_id", 123)
        expected["user_id"] = 123
        executor_options = self.action._executor_options(input_data_key=None,
                net=self.stored_net)
        self.assertEqual(expected, executor_options)

        self.stored_net.set_constant("working_directory", "/tmp")
        expected["working_directory"] = "/tmp"
        executor_options = self.action._executor_options(input_data_key=None,
                net=self.stored_net)
        self.assertEqual(expected, executor_options)

        self.stored_net.set_constant("mail_user", "foo@bar.com")
        expected["mail_user"] = "foo@bar.com"
        executor_options = self.action._executor_options(input_data_key=None,
                net=self.stored_net)
        self.assertEqual(expected, executor_options)

class TestLsfDispatchAction(TestBase, _TestDispatchActionMixIn):
    net_class = enets.LSFCommandNet
    action_class = enets.LSFDispatchAction

    def test_response_places(self):
        self.assertEqual("dispatch", str(self.dispatch_transition.name))
        self.assertIsInstance(self.action, self.action_class)

        expected = {
            'post_dispatch_success': self.net.dispatch_success_place.index,
            'post_dispatch_failure': self.net.dispatch_failure_place.index,
            'begin_execute': self.net.begin_execute_place.index,
            'execute_success': self.net.execute_success_place.index,
            'execute_failure': self.net.execute_failure_place.index,
        }

        response_places = self.action._response_places()
        self.assertEqual(expected, response_places)


class TestLocalDispatchAction(TestBase, _TestDispatchActionMixIn):
    net_class = enets.LocalCommandNet
    action_class = enets.LocalDispatchAction

    def test_response_places(self):
        expected = {
            'begin_execute': self.net.on_begin_execute.index,
            'execute_success': self.net.on_execute_success.index,
            'execute_failure': self.net.on_execute_failure.index,
        }

        self.assertEqual("dispatch", str(self.dispatch_transition.name))
        self.assertIsInstance(self.action, enets.LocalDispatchAction)

        response_places = self.action._response_places()
        self.assertEqual(expected, response_places)


if __name__ == "__main__":
    unittest.main()
