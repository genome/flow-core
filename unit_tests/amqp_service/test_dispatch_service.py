import unittest
try:
    from unittest import mock
except:
    import mock

from flow.amqp_service.dispatch_service import DispatchService


class DispatchServiceTest(unittest.TestCase):
    def setUp(self):
        self.exchange_manager = mock.Mock()
        self.exchange_manager.publish = mock.Mock()

        self.properties = mock.Mock()

        self.command_line = mock.Mock()
        self.return_identifier = mock.Mock()
        self.success_routing_key = mock.Mock()
        self.failure_routing_key = mock.Mock()
        self.error_routing_key = mock.Mock()

        self.working_directory = mock.Mock()

        self.data = {'command_line': self.command_line,
                     'return_identifier': self.return_identifier,
                     'success_routing_key': self.success_routing_key,
                     'failure_routing_key': self.failure_routing_key,
                     'error_routing_key': self.error_routing_key,
                     'working_directory': self.working_directory}

        self.dispatcher = mock.Mock()
        self.dispatcher.launch_job = mock.Mock()

        self.publish_properties = {'passthru': mock.Mock()}

        self.responder = DispatchService(self.dispatcher,
                self.exchange_manager, **self.publish_properties)


    def test_on_message_success(self):
        expected_job_id = mock.Mock()
        self.dispatcher.launch_job.return_value = True, expected_job_id

        ack_callback = mock.Mock()
        reject_callback = mock.Mock()
        self.responder.message_handler(self.properties, self.data,
                ack_callback, reject_callback)

        self.dispatcher.launch_job.assert_called_once_with(
                self.command_line, working_directory=self.working_directory,
                environment={}, stdout=None, stderr=None)

        self.exchange_manager.publish.assert_called_once_with(
                self.success_routing_key,
                {'return_identifier': self.return_identifier,
                    'dispatch_result': expected_job_id},
                **self.publish_properties)


    def test_on_message_failure(self):
        failure_output = mock.Mock()
        self.dispatcher.launch_job.return_value = False, failure_output

        ack_callback = mock.Mock()
        reject_callback = mock.Mock()
        self.responder.message_handler(self.properties, self.data,
                ack_callback, reject_callback)

        self.dispatcher.launch_job.assert_called_once_with(
                self.command_line, working_directory=self.working_directory,
                environment={}, stdout=None, stderr=None)

        self.exchange_manager.publish.assert_called_once_with(
                self.failure_routing_key,
                {'return_identifier': self.return_identifier,
                    'dispatch_result': failure_output},
                **self.publish_properties)


    def test_on_message_serious_error(self):
        self.dispatcher.launch_job.side_effect = Exception('test exception')

        ack_callback = mock.Mock()
        reject_callback = mock.Mock()

        self.assertRaises(Exception, self.responder.message_handler,
                self.properties, self.data, ack_callback, reject_callback)

        self.dispatcher.launch_job.assert_called_once_with(
                self.command_line, working_directory=self.working_directory,
                environment={}, stdout=None, stderr=None)


    def test_on_message_error(self):
        e = RuntimeError('test exception')
        self.dispatcher.launch_job.side_effect = e

        ack_callback = mock.Mock()
        reject_callback = mock.Mock()
        self.responder.message_handler(self.properties, self.data,
                ack_callback, reject_callback)

        self.dispatcher.launch_job.assert_called_once_with(
                self.command_line, working_directory=self.working_directory,
                environment={}, stdout=None, stderr=None)

        self.exchange_manager.publish.assert_called_once_with(
                self.error_routing_key,
                {'return_identifier': self.return_identifier,
                    'dispatch_result': str(e)},
                **self.publish_properties)


if '__main__' == __name__:
    unittest.main()
