import unittest
try:
    from unittest import mock
except:
    import mock

from flow_command_runner.messages import CommandLineResponseMessage
from flow_command_runner.handler import CommandLineSubmitMessageHandler

class CommandLineSubmitMessageHandlerTest(unittest.TestCase):
    def setUp(self):
        self.executor = mock.Mock()

        self.broker = mock.Mock()
        self.broker.publish = mock.Mock()

        self.handler = CommandLineSubmitMessageHandler(
                self.executor, self.broker)

        self.message = mock.Mock()
        self.message.command_line = mock.Mock()
        self.message.success_routing_key = 'succes_key'
        self.message.failure_routing_key = 'failure_key'
        self.message.error_routing_key = 'error_key'
        self.message.executor_options = {'passthru': True}


    def test_message_handler_executor_success(self):
        executor_result = 'my_job_id'
        self.executor.return_value = (True, executor_result)
        self.handler(self.message)
        self.executor.assert_called_once_with(self.message.command_line,
                passthru=True)

        response_message = CommandLineResponseMessage(
                return_identifier=self.message.return_identifier,
                job_id=executor_result)
        self.broker.publish.assert_called_once_with(
                self.message.success_routing_key, response_message)

    def test_message_handler_executor_failure(self):
        executor_result = mock.Mock()
        self.executor.return_value = (False, executor_result)
        self.handler(self.message)
        self.executor.assert_called_once_with(self.message.command_line,
                passthru=True)

        response_message = CommandLineResponseMessage(
                return_identifier=self.message.return_identifier)
        self.broker.publish.assert_called_once_with(
                self.message.failure_routing_key, response_message)


    def test_message_handler_executor_exception(self):
        self.executor.side_effect = RuntimeError('error_message')
        self.handler(self.message)
        self.executor.assert_called_once_with(self.message.command_line,
                passthru=True)

        response_message = CommandLineResponseMessage(
                return_identifier=self.message.return_identifier,
                error_message = 'error_message')
        self.broker.publish.assert_called_once_with(
                self.message.error_routing_key, response_message)


if '__main__' == __name__:
    unittest.main()
