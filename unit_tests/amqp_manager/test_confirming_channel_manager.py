import unittest
try:
    from unittest import mock
except:
    import mock

import copy
import pika

from amqp_manager import confirming_channel_manager


class ConfirmingChannelManagerSetupTest(unittest.TestCase):
    def test_on_channel_open(self):
        channel = mock.Mock()
        channel.confirm_delivery = mock.Mock()

        cm = confirming_channel_manager.ConfirmingChannelManager()
        cm._setup_channel = mock.Mock()
        cm._inform_delegates_about_channel = mock.Mock()

        with mock.patch.object(confirming_channel_manager,
                'add_confirm_ack_callback') as add_ack:
            with mock.patch.object(confirming_channel_manager,
                    'add_confirm_nack_callback') as add_nack:
                cm._on_channel_open(channel)
                add_ack.assert_called_once_with(channel, cm.on_confirm_ack)
                add_nack.assert_called_once_with(channel, cm.on_confirm_nack)

        cm._setup_channel.assert_called_once_with(channel)
        channel.confirm_delivery.assert_called_once_with()
        cm._inform_delegates_about_channel.assert_called_once_with(channel)

    def test_add_confirm_ack_callback(self):
        confirm_callback_helper(
                confirming_channel_manager.add_confirm_ack_callback,
                pika.spec.Basic.Ack)

    def test_add_confirm_nack_callback(self):
        confirm_callback_helper(
                confirming_channel_manager.add_confirm_nack_callback,
                pika.spec.Basic.Nack)

def confirm_callback_helper(callback_adder, basic_spec):
    channel = mock.Mock()
    channel.callbacks = mock.Mock()
    channel.callbacks.add = mock.Mock()

    channel.number = mock.Mock()
    callback = mock.Mock()

    callback_adder(channel, callback)
    channel.callbacks.add.assert_called_once_with(
            channel.number, basic_spec, callback, one_shot=False)


class ConfirmingChannelManagerPublishTest(unittest.TestCase):
    def setUp(self):
        self.cm = confirming_channel_manager.ConfirmingChannelManager()

        self.delivery_tag = mock.Mock()
        self.cm.basic_publish = mock.Mock()
        self.cm.basic_publish.return_value = self.delivery_tag

        self.basic_publish_properties = {
                'exchange_name': mock.Mock(),
                'routing_key': mock.Mock(),
                'message': mock.Mock(),
                'passthru_property': mock.Mock(),
                }

    def test_publish_first_attempt(self):
        result = self.cm.publish(**self.basic_publish_properties)

        self.assertEqual(result, self.delivery_tag)

        self.cm.basic_publish.assert_called_once_with(
                **self.basic_publish_properties)

        expected_entry = copy.copy(self.basic_publish_properties)
        expected_entry['attempts'] = 1

        self.assertEqual(1, len(self.cm._unconfirmed_messages))
        self.assertEqual(expected_entry,
                self.cm._unconfirmed_messages[self.delivery_tag])

    def test_publish_too_many_attempts_no_callback(self):
        result = self.cm.publish(attempts=self.cm.max_publish_attempts+1,
                **self.basic_publish_properties)
        self.assertEqual(result, None)

    def test_publish_too_many_attempts_with_callback(self):
        failure_callback = mock.Mock()
        failure_callback.return_value = mock.Mock()

        result = self.cm.publish(attempts=self.cm.max_publish_attempts+1,
                failure_callback=failure_callback,
                **self.basic_publish_properties)
        self.assertEqual(result, failure_callback.return_value)
        failure_callback.assert_called_once_with()


class ConfirmingChannelManagerConfirmTest(unittest.TestCase):
    def setUp(self):
        self.delivery_tag = mock.Mock()
        self.method_frame = mock.Mock()
        self.method_frame.method = mock.Mock()
        self.method_frame.method.delivery_tag = self.delivery_tag

        bpp = {
                'exchange_name': mock.Mock(),
                'routing_key': mock.Mock(),
                'message': mock.Mock(),
                'success_callback': mock.Mock(),
                'failure_callback': mock.Mock(),
                'passthru_property': mock.Mock(),
                }

        self.basic_publish_properties = bpp
        self.cm = confirming_channel_manager.ConfirmingChannelManager()
        self.cm._unconfirmed_messages[self.delivery_tag] = bpp

    def test_on_confirm_ack(self):
        bpp = self.basic_publish_properties
        self.cm._unconfirmed_messages[self.delivery_tag] = bpp

        self.cm.on_confirm_ack(self.method_frame)
        self.assertEqual(0, len(self.cm._unconfirmed_messages))
        bpp['success_callback'].assert_called_once_with()


    def test_on_confirm_nack(self):
        self.cm.basic_publish = mock.Mock()

        self.cm.on_confirm_nack(self.method_frame)

        self.assertEqual(0, len(self.cm._unconfirmed_messages))

        bpp = self.basic_publish_properties
        self.cm.basic_publish.assert_called_once_with(**bpp)


if '__main__' == __name__:
    unittest.main()
