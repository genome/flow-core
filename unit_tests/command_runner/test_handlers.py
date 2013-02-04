import unittest
try:
    from unittest import mock
except:
    import mock

from flow.petri.safenet import SetTokenMessage

import flow.command_runner.handler
from flow.command_runner.handler import CommandLineSubmitMessageHandler

class CommandLineSubmitMessageHandlerTest(unittest.TestCase):
    def setUp(self):
        self.executor = mock.Mock()

        self.broker = mock.Mock()
        self.broker.publish = mock.Mock()
        self.routing_key = mock.Mock()

        self.storage = mock.Mock()

        self.handler = CommandLineSubmitMessageHandler(executor=self.executor,
                broker=self.broker, storage=self.storage,
                routing_key=self.routing_key)

        self.net_key = mock.Mock(str)
        self.dispatch_success_place_idx = mock.Mock(int)
        self.dispatch_failure_place_idx = mock.Mock(int)
        self.response_places = {
                'dispatch_success': self.dispatch_success_place_idx,
                'dispatch_failure': self.dispatch_failure_place_idx
        }

        self.message = mock.Mock()
        self.message.command_line = mock.Mock()
        self.message.net_key = self.net_key
        self.message.response_places = self.response_places
        self.message.executor_options = {'passthru': True}


    def test_message_handler_executor_success(self):
        executor_result = 'my_job_id'
        self.executor.return_value = (True, executor_result)
        with mock.patch("flow.command_runner.handler.Token") as T:
            T.create = mock.Mock()
            token = mock.Mock
            token.key = mock.Mock(str)
            T.create.return_value = token

            self.handler(self.message)

            self.executor.assert_called_once_with(self.message.command_line,
                    net_key=self.net_key, response_places=self.response_places,
                    passthru=True)

            T.create.assert_called_once_with(self.storage)

            response_message = SetTokenMessage(token_key=token.key,
                    net_key=self.net_key,
                    place_idx=self.dispatch_success_place_idx)
            self.broker.publish.assert_called_once_with(
                    self.routing_key, response_message)

    def test_message_handler_executor_failure(self):
        executor_result = mock.Mock()
        self.executor.return_value = (False, executor_result)

        with mock.patch("flow.command_runner.handler.Token") as T:
            T.create = mock.Mock()
            token = mock.Mock
            token.key = mock.Mock(str)
            T.create.return_value = token

            self.handler(self.message)

            self.executor.assert_called_once_with(self.message.command_line,
                    net_key=self.net_key, response_places=self.response_places,
                    passthru=True)

            T.create.assert_called_once_with(self.storage)

            response_message = SetTokenMessage(token_key=token.key,
                    net_key=self.net_key,
                    place_idx=self.dispatch_failure_place_idx)
            self.broker.publish.assert_called_once_with(
                    self.routing_key, response_message)



    def test_message_handler_executor_exception(self):
        self.executor.side_effect = RuntimeError('error_message')

        with mock.patch("flow.command_runner.handler.Token") as T:
            T.create = mock.Mock()
            token = mock.Mock
            token.key = mock.Mock(str)
            T.create.return_value = token

            self.handler(self.message)

            self.executor.assert_called_once_with(self.message.command_line,
                    net_key=self.net_key, response_places=self.response_places,
                    passthru=True)

            T.create.assert_called_once_with(self.storage)

            response_message = SetTokenMessage(token_key=token.key,
                    net_key=self.net_key,
                    place_idx=self.dispatch_failure_place_idx)
            self.broker.publish.assert_called_once_with(
                    self.routing_key, response_message)


if '__main__' == __name__:
    unittest.main()
