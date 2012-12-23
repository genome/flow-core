import unittest
try:
    from unittest import mock
except:
    import mock

from amqp_manager import queue_manager


class QueueManagerSetupTest(unittest.TestCase):
    def setUp(self):
        self.queue_name = mock.Mock()
        self.decoder = mock.Mock()
        self.durable = mock.Mock()

        self.bad_data_handler = mock.Mock()
        self.message_handler = mock.Mock()

        self.qm = queue_manager.QueueManager(self.queue_name,
                decoder=self.decoder, durable=self.durable,
                bad_data_handler=self.bad_data_handler,
                message_handler=self.message_handler)


    def test_on_channel_open(self):
        channel = mock.Mock()
        channel.queue_declare = mock.Mock()

        channel_manager = mock.Mock()

        self.qm.on_channel_open(channel_manager, channel)
        self.assertEqual(channel, self.qm._channel)
        channel.queue_declare.assert_called_once_with(
                self.qm._on_declare_queue_ok, self.queue_name,
                durable=self.durable)


    def test_on_channel_closed(self):
        channel = mock.Mock()
        self.qm.on_channel_closed(channel)
        self.assertEqual(None, self.qm._channel)
        self.assertEqual(None, self.qm.queue)

    def test_on_declare_queue_ok(self):
        queue = mock.Mock()
        method_frame = mock.Mock()
        channel = mock.Mock()
        channel.basic_consume = mock.Mock()

        self.qm._channel = channel

        self.qm._on_declare_queue_ok(queue, method_frame)
        channel.basic_consume.assert_called_once_with(self.qm.on_message, queue)


class QueueManagerMessageTest(unittest.TestCase):
    def setUp(self):
        self.decoder = mock.Mock()
        self.decoded_message = mock.Mock()
        self.decoder.return_value = self.decoded_message

        self.bad_data_handler = mock.Mock()
        self.message_handler = mock.Mock()

        self.qm = queue_manager.QueueManager(None, decoder=self.decoder,
                bad_data_handler=self.bad_data_handler,
                message_handler=self.message_handler)

        self.ack_callback = mock.Mock()
        self.reject_callback = mock.Mock()

        self.channel = mock.Mock()
        self.channel.basic_ack = mock.Mock()
        self.channel.basic_reject = mock.Mock()

        self.basic_deliver = mock.Mock()
        self.properties = mock.Mock()
        self.body = mock.Mock()


    def call_on_message_with_substitute_callbacks(self):
        with mock.patch.object(queue_manager, 'make_ack_callback') as ac:
            ac.return_value = self.ack_callback
            with mock.patch.object(queue_manager, 'make_reject_callback') as rc:
                rc.return_value = self.reject_callback
                self.qm.on_message(self.channel, self.basic_deliver,
                        self.properties, self.body)
                ac.assert_called_once_with(self.channel, self.basic_deliver)
                rc.assert_called_once_with(self.channel, self.basic_deliver)


    def test_on_message_decoder_throws(self):
        self.decoder.side_effect = RuntimeError
        self.call_on_message_with_substitute_callbacks()
        self.decoder.assert_called_once_with(self.body)

        # In practice, this might be called more than once (if bad_data_handler
        # calls it).
        self.reject_callback.assert_called_once_with()

    def test_on_message_bad_data_handler_throws(self):
        self.decoder.side_effect = RuntimeError
        self.bad_data_handler.side_effect = RuntimeError
        self.call_on_message_with_substitute_callbacks()
        self.decoder.assert_called_once_with(self.body)

        self.reject_callback.assert_called_once_with()


    def test_on_message_handler_throws(self):
        self.message_handler.side_effect = RuntimeError
        self.call_on_message_with_substitute_callbacks()
        self.decoder.assert_called_once_with(self.body)
        self.message_handler.assert_called_once_with(self.decoded_message,
                self.ack_callback, self.reject_callback)
        self.reject_callback.assert_called_once_with()

    def test_on_message_normal(self):
        self.call_on_message_with_substitute_callbacks()
        self.decoder.assert_called_once_with(self.body)
        self.message_handler.assert_called_once_with(self.decoded_message,
                self.ack_callback, self.reject_callback)


if '__main__' == __name__:
    unittest.main()
