import unittest
try:
    from unittest import mock
except:
    import mock

from amqp_service.responder.dispatch_responder import DispatchResponder


class DispatchResponderTest(unittest.TestCase):
    def setUp(self):
        self.queue = mock.Mock()
        self.durable_queue = mock.Mock()
        self.exchange = mock.Mock()
        self.exchange_type = mock.Mock()
        self.prefetch_count = mock.Mock()

        self.channel = mock.Mock()
        self.basic_deliver = mock.Mock()
        self.properties = mock.Mock()


        self.command_value = mock.Mock()
        self.return_identifier = mock.Mock()
        self.success_routing_key = mock.Mock()
        self.failure_routing_key = mock.Mock()
        self.error_routing_key = mock.Mock()

        self.data = {'command': self.command_value,
                     'return_identifier': self.return_identifier,
                     'success_routing_key': self.success_routing_key,
                     'failure_routing_key': self.failure_routing_key,
                     'error_routing_key': self.error_routing_key}

        self.arg_value = mock.Mock()

        self.dispatcher = mock.Mock()
        self.dispatcher.launch_job = mock.Mock()

        self.responder = DispatchResponder(self.dispatcher,
                durable_queue=self.durable_queue,
                exchange=self.exchange,
                exchange_type=self.exchange_type,
                prefetch_count=self.prefetch_count)


    def test_on_message_success(self):
        expected_job_id = mock.Mock()
        self.dispatcher.launch_job.return_value = True, expected_job_id

        routing_key, output_data = self.responder.on_message(self.channel,
                self.basic_deliver, self.properties, self.data)

        self.dispatcher.launch_job.assert_called_once_with(
                self.command_value, arguments=[],
                environment={})

        self.assertEqual(routing_key, self.success_routing_key)
        self.assertEqual(output_data,
                {'return_identifier': self.return_identifier,
                 'dispatch_result': expected_job_id})


    def test_on_message_failure(self):
        failure_output = mock.Mock()
        self.dispatcher.launch_job.return_value = False, failure_output

        routing_key, output_data = self.responder.on_message(self.channel,
                self.basic_deliver, self.properties, self.data)

        self.dispatcher.launch_job.assert_called_once_with(
                self.command_value, arguments=[],
                environment={})

        self.assertEqual(routing_key, self.failure_routing_key)
        self.assertEqual(output_data,
                {'return_identifier': self.return_identifier,
                 'dispatch_result': failure_output})


    def test_on_message_serious_error(self):
        self.dispatcher.launch_job.side_effect = Exception('test exception')

        self.assertRaises(Exception, self.responder.on_message,
                self.channel, self.basic_deliver, self.properties, self.data)

        self.dispatcher.launch_job.assert_called_once_with(
                self.command_value, arguments=[],
                environment={})


    def test_on_message_error(self):
        e = RuntimeError('test exception')
        self.dispatcher.launch_job.side_effect = e

        routing_key, output_data = self.responder.on_message(self.channel,
                self.basic_deliver, self.properties, self.data)

        self.assertEqual(routing_key, self.error_routing_key)
        self.assertEqual(output_data,
                {'return_identifier': self.return_identifier,
                 'dispatch_result': str(e)})

        self.dispatcher.launch_job.assert_called_once_with(
                self.command_value, arguments=[],
                environment={})



if '__main__' == __name__:
    unittest.main()
