import unittest
try:
    from unittest import mock
except:
    import mock

from flow.petri import SetTokenMessage

import flow.shell_command.handler
from flow.shell_command.handler import LSFShellCommandMessageHandler

class ShellCommandMessageHandlerTest(unittest.TestCase):
    def setUp(self):
        self.executor = mock.Mock()

        orchestrator = mock.Mock()
        self.set_token = mock.Mock()
        orchestrator.set_token = self.set_token

        service_locator = {'orchestrator':orchestrator}

        self.exchange = mock.Mock()
        self.routing_key = mock.Mock()

        self.storage = mock.Mock()

        self.handler = LSFShellCommandMessageHandler(executor=self.executor,
                storage=self.storage, queue_name='',
                service_locator=service_locator,
                exchange=self.exchange,
                response_routing_key=self.routing_key)

        self.net_key = mock.Mock(str)
        self.dispatch_success_place_idx = 1
        self.dispatch_failure_place_idx = 2
        self.response_places = {
                'post_dispatch_success': self.dispatch_success_place_idx,
                'post_dispatch_failure': self.dispatch_failure_place_idx
        }

        self.message = mock.Mock()
        self.message.command_line = mock.Mock()
        self.message.net_key = self.net_key
        self.message.response_places = self.response_places
        self.message.executor_options = {'passthru': True}

    def test_message_handler_executor_success(self):
        job_id = 1234
        self.executor.return_value = (job_id, True)

        self.message.token_color = None
        self.handler(self.message)

        self.executor.assert_called_once_with(self.message.command_line,
                net_key=self.net_key, response_places=self.response_places,
                token_color=self.message.token_color, passthru=True)

        self.set_token.assert_any_call(net_key=self.net_key, token_color=None,
                place_idx=self.dispatch_success_place_idx, token_key=mock.ANY)

    def test_message_handler_executor_failure(self):
        job_id = 1234
        self.executor.return_value = (job_id, False)

        self.message.token_color = 3
        self.handler(self.message)

        self.executor.assert_called_once_with(self.message.command_line,
                net_key=self.net_key, response_places=self.response_places,
                token_color=self.message.token_color,
                passthru=True)

        self.set_token.assert_any_call(net_key=self.net_key, token_color=3,
                place_idx=self.dispatch_failure_place_idx, token_key=mock.ANY)

    def test_message_handler_executor_exception(self):
        self.executor.side_effect = RuntimeError

        with mock.patch("flow.shell_command.handler.Token") as T:
            T.create = mock.Mock()
            token = mock.Mock
            token.key = mock.Mock(str)
            T.create.return_value = token

            self.assertRaises(RuntimeError, self.handler, self.message)

            self.executor.assert_called_once_with(self.message.command_line,
                    net_key=self.net_key, response_places=self.response_places,
                    token_color=self.message.token_color, passthru=True)


if '__main__' == __name__:
    unittest.main()
