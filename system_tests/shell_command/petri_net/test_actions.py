from flow.petri_net import builder
from flow.petri_net.future import FutureAction
from flow.shell_command.petri_net import actions
from flow.shell_command.petri_net import future_nets

import os
import test_helpers
import unittest


class TestBase(test_helpers.RedisTest):
    def setUp(self):
        test_helpers.RedisTest.setUp(self)

        self.cmdline = ["ls", "-al"]
        self.builder = builder.Builder(self.conn)

        self.net = self.net_class(name="test", command_line=self.cmdline)
        self.future_places, self.future_transitions = \
                builder.gather_nodes(self.net)

        self.stored_net = self.builder.store(self.net, {}, {})

        self.dispatch_transition = self.stored_net.transition(
                self.future_transitions[self.net.dispatch])
        self.action = self.dispatch_transition.action


class _TestDispatchActionMixIn(object):
    def test_command_line(self):
        self.assertEqual(self.cmdline, self.action.command_line(None))

    def test_inputs_hash_key(self):
        self.assertIs(None, self.action.inputs_hash_key(None))

    def test_executor_options(self):
        executor_options = self.action._executor_options(self.stored_net)
        self.assertEqual({}, executor_options)

        env = os.environ.data
        self.stored_net.set_constant('environment', env)

        expected = {'environment': env}
        executor_options = self.action._executor_options(self.stored_net)
        self.assertEqual(expected, executor_options)

        self.stored_net.set_constant("user_id", 123)
        expected["user_id"] = 123
        executor_options = self.action._executor_options(self.stored_net)
        self.assertEqual(expected, executor_options)

        self.stored_net.set_constant("working_directory", "/tmp")
        expected["working_directory"] = "/tmp"
        executor_options = self.action._executor_options(self.stored_net)
        self.assertEqual(expected, executor_options)

        self.stored_net.set_constant("mail_user", "foo@bar.com")
        expected["mail_user"] = "foo@bar.com"
        executor_options = self.action._executor_options(self.stored_net)
        self.assertEqual(expected, executor_options)


class TestLsfDispatchAction(TestBase, _TestDispatchActionMixIn):
    net_class = future_nets.LSFCommandNet
    action_class = actions.LSFDispatchAction

    def test_response_places(self):
        self.assertEqual("dispatch", str(self.dispatch_transition.name))
        self.assertIsInstance(self.action, self.action_class)

        expected = {
            'msg: dispatch_success': self.future_places[
                self.net.dispatch_success_place],
            'msg: dispatch_failure': self.future_places[
                self.net.dispatch_failure_place],
            'msg: execute_begin': self.future_places[
                self.net.execute_begin_place],
            'msg: execute_success': self.future_places[
                self.net.execute_success_place],
            'msg: execute_failure': self.future_places[
                self.net.execute_failure_place],
        }

        response_places = self.action._response_places()
        self.assertEqual(expected, response_places)


class TestForkDispatchAction(TestBase, _TestDispatchActionMixIn):
    net_class = future_nets.ForkCommandNet
    action_class = actions.ForkDispatchAction

    def test_response_places(self):
        expected = {
            'msg: dispatch_success': self.future_places[
                self.net.dispatch_success_place],
            'msg: dispatch_failure': self.future_places[
                self.net.dispatch_failure_place],
            'msg: execute_begin': self.future_places[
                self.net.execute_begin_place],
            'msg: execute_success': self.future_places[
                self.net.execute_success_place],
            'msg: execute_failure': self.future_places[
                self.net.execute_failure_place],
        }

        self.assertEqual("dispatch", str(self.dispatch_transition.name))
        self.assertIsInstance(self.action, actions.ForkDispatchAction)

        response_places = self.action._response_places()
        self.assertEqual(expected, response_places)


if __name__ == "__main__":
    unittest.main()
