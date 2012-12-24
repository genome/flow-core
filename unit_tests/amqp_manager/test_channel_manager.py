import unittest
try:
    from unittest import mock
except:
    import mock

import pika
from amqp_manager import channel_manager

class ChanelManagerPrefetchTest(unittest.TestCase):
    def test_setup_channel(self):
        prefetch_count = mock.Mock()
        cm = channel_manager.ChannelManager(prefetch_count=prefetch_count)

        channel = mock.Mock()
        channel.add_on_close_callback = mock.Mock()
        channel.basic_qos = mock.Mock()
        cm._setup_channel(channel)

        channel.add_on_close_callback.assert_called_once_with(
                cm._on_channel_closed)

        channel.basic_qos.assert_called_once_with(
                prefetch_count=prefetch_count)


class ChannelManagerDelegationTest(unittest.TestCase):
    def setUp(self):
        self.delegates = [mock.Mock(), mock.Mock()]
        for delegate in self.delegates:
            delegate.on_channel_open = mock.Mock()
            delegate.on_channel_closed = mock.Mock()

        self.channel_manager = channel_manager.ChannelManager(
                delegates=self.delegates)

    def test_on_channel_open(self):
        channel = mock.Mock()
        channel.add_on_close_callback = mock.Mock()

        self.channel_manager._on_channel_open(channel)
        self.assertEqual(channel, self.channel_manager._channel)

        channel.add_on_close_callback.assert_called_once_with(
                self.channel_manager._on_channel_closed)
        for delegate in self.delegates:
            delegate.on_channel_open.assert_called_once_with(
                    self.channel_manager)

    def test_on_channel_closed(self):
        channel = mock.Mock()

        self.channel_manager._channel = channel
        self.channel_manager._on_channel_closed(channel)
        for delegate in self.delegates:
            delegate.on_channel_closed.assert_called_once_with(channel)
        self.assertEqual(None, self.channel_manager._channel)


class ChannelManagerConnectionTest(unittest.TestCase):
    def setUp(self):
        self.connection = mock.Mock()
        self.connection.channel = mock.Mock()

        self.channel_manager = channel_manager.ChannelManager()

    def test_on_connection_open(self):
        self.channel_manager.on_connection_open(self.connection)

        self.connection.channel.assert_called_once_with(
                self.channel_manager._on_channel_open)


class ChannelManagerPublishTest(unittest.TestCase):
    def setUp(self):
        self.channel_manager = channel_manager.ChannelManager()

        self.basic_publish_properties = {
                'exchange_name': mock.Mock(),
                'routing_key': mock.Mock(),
                'message': mock.Mock(),
                'passthru_property': mock.Mock(),
                }


    def test_nonpersistent_basic_publish(self):
        self.channel_manager._channel = mock.Mock()
        self.channel_manager._channel.basic_publish = mock.Mock()

        bpp = self.basic_publish_properties
        self.channel_manager.basic_publish(persistent=False, **bpp)

        self.channel_manager._channel.basic_publish.assert_called_once_with(
                bpp['exchange_name'], bpp['routing_key'], bpp['message'],
                properties=None, passthru_property=bpp['passthru_property'])

    def test_persistent_basic_publish(self):
        self.channel_manager._channel = mock.Mock()
        self.channel_manager._channel.basic_publish = mock.Mock()

        bpp = self.basic_publish_properties
        with mock.patch.object(channel_manager, 'pika') as pika:
            properties = mock.Mock()
            pika.BasicProperties = mock.Mock()
            pika.BasicProperties.return_value = properties

            self.channel_manager.basic_publish(persistent=True, **bpp)

            pika.BasicProperties.assert_called_once_with(delivery_mode=2)

            self.channel_manager._channel.basic_publish.assert_called_once_with(
                    bpp['exchange_name'], bpp['routing_key'], bpp['message'],
                    properties=properties,
                    passthru_property=bpp['passthru_property'])


    def test_publish_no_callbacks(self):
        self.channel_manager.basic_publish = mock.Mock()

        self.channel_manager.publish(**self.basic_publish_properties)

        self.channel_manager.basic_publish.assert_called_once_with(
                **self.basic_publish_properties)

    def test_publish_success_callback(self):
        self.channel_manager.basic_publish = mock.Mock()
        self.channel_manager.basic_publish.return_value = 42

        success_callback = mock.Mock()

        self.channel_manager.publish(success_callback=success_callback,
                **self.basic_publish_properties)

        self.channel_manager.basic_publish.assert_called_once_with(
                **self.basic_publish_properties)
        success_callback.assert_called_once_with()

    def test_publish_failure_callback(self):
        self.channel_manager.basic_publish = mock.Mock()
        self.channel_manager.basic_publish.return_value = 0
        failure_callback = mock.Mock()

        self.channel_manager.publish(failure_callback=failure_callback,
                **self.basic_publish_properties)

        self.channel_manager.basic_publish.assert_called_once_with(
                **self.basic_publish_properties)
        failure_callback.assert_called_once_with()


class ChannelManagerMessageCallbackTest(unittest.TestCase):
    # Could add tests for exchange/queue declare and basic_consume
    def setUp(self):
        self.on_message_callback = mock.Mock()

        self.channel = mock.Mock()

        self.basic_deliver = mock.Mock()
        self.properties = mock.Mock()
        self.body = mock.Mock()

        self.cm = channel_manager.ChannelManager()

        self.ack_callback = mock.Mock()
        self.reject_callback = mock.Mock()

    def test_normal_callback(self):
        self.call_on_message_with_substitute_callbacks()

        self.on_message_callback.assert_called_once_with(self.properties,
                self.body, self.ack_callback, self.reject_callback)

    def test_callback_throws(self):
        self.on_message_callback.side_effect = RuntimeError
        self.call_on_message_with_substitute_callbacks()

        self.on_message_callback.assert_called_once_with(self.properties,
                self.body, self.ack_callback, self.reject_callback)

        self.reject_callback.assert_called_once_with()


    def call_on_message_with_substitute_callbacks(self):
        with mock.patch.object(channel_manager, 'make_ack_callback') as ac:
            ac.return_value = self.ack_callback
            with mock.patch.object(channel_manager,
                    'make_reject_callback') as rc:
                rc.return_value = self.reject_callback
                self.cm._on_message_callback( self.on_message_callback,
                        self.channel, self.basic_deliver,
                        self.properties, self.body)
                ac.assert_called_once_with(self.channel, self.basic_deliver)
                rc.assert_called_once_with(self.channel, self.basic_deliver)


if '__main__' == __name__:
    unittest.main()
