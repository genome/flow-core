import unittest
try:
    from unittest import mock
except:
    import mock

from amqp_service.responder.grid_submit import GridSubmitResponder


class GridSubmitResponderTest(unittest.TestCase):
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
        self.arg_value = mock.Mock()
        self.data = {'fields': {'params': {
            'command': self.command_value, 'arg': self.arg_value}}}

        self.dispatcher = mock.Mock()

        self.succeeded_routing_key = mock.Mock()

        self.responder = GridSubmitResponder(self.dispatcher,
                succeeded_routing_key=self.succeeded_routing_key,
                queue=self.queue,
                durable_queue=self.durable_queue,
                exchange=self.exchange,
                exchange_type=self.exchange_type,
                prefetch_count=self.prefetch_count)


    def test_on_message_success(self):
        expected_job_id = mock.Mock()
        self.dispatcher.launch_job = mock.Mock()
        self.dispatcher.launch_job.return_value = expected_job_id

        routing_key, output_data = self.responder.on_message(self.channel,
                self.basic_deliver, self.properties, self.data)

        self.dispatcher.launch_job.assert_called_once_with(
                self.command_value, self.arg_value)

        self.assertEqual(routing_key, self.succeeded_routing_key)
        self.assertEqual(output_data, {'job_id': expected_job_id})

    def test_on_message_error(self):
        self.dispatcher.launch_job = mock.Mock()
        self.dispatcher.launch_job.side_effect = RuntimeError('test exception')

        self.assertRaises(RuntimeError, self.responder.on_message,
                self.channel, self.basic_deliver, self.properties, self.data)

        self.dispatcher.launch_job.assert_called_once_with(
                self.command_value, self.arg_value)


if '__main__' == __name__:
    unittest.main()
