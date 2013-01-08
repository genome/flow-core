import unittest
try:
    from unittest import mock
except:
    import mock

from amqp_manager import exchange_manager


class ExchangeManagerSetupTest(unittest.TestCase):
    def setUp(self):
        self.exchange_name = mock.Mock()
        self.exchange_type = mock.Mock()
        self.durable = mock.Mock()
        self.ed_arguments = {
                'passthru': mock.Mock(),
                }
        self.em = exchange_manager.ExchangeManager(
                self.exchange_name, exchange_type=self.exchange_type,
                durable=self.durable, **self.ed_arguments)

        self.channel_manager = mock.Mock()

    def test_on_channel_open(self):
        self.channel_manager.exchange_declare = mock.Mock()

        self.em.on_channel_open(self.channel_manager)
        self.assertEqual(self.channel_manager, self.em._channel_manager)
        self.channel_manager.exchange_declare.assert_called_once_with(
                self.em._on_exchange_declare_ok, self.exchange_name,
                exchange_type=self.exchange_type,
                durable=self.durable, arguments=self.ed_arguments)

    def test_on_channel_closed(self):
        channel = mock.Mock()
        self.em.on_channel_closed(channel)
        self.assertEqual(None, self.em._channel_manager)

    def test_exchange_declare_ok(self):
        method_frame = mock.Mock()
        self.em.notify_ready = mock.Mock()
        self.em._on_exchange_declare_ok(method_frame)

        self.em.notify_ready.assert_called_once_with()


class ExchangeManagerPublishTest(unittest.TestCase):
    def setUp(self):
        self.exchange_name = mock.Mock()
        self.basic_publish_properties = {'passthru': mock.Mock()}
        self.persistent = mock.Mock()
        self.em = exchange_manager.ExchangeManager(self.exchange_name,
                basic_publish_properties=self.basic_publish_properties,
                persistent=self.persistent)

        self.channel_manager = mock.Mock()
        self.channel_manager.publish = mock.Mock()

        self.em._channel_manager = self.channel_manager

        self.success_callback = mock.Mock()
        self.failure_callback = mock.Mock()

        self.routing_key = mock.Mock()
        self.unencoded_message = mock.Mock()

    def test_publish_normal(self):
        self.em.publish(self.routing_key, self.unencoded_message)

        self.em._channel_manager.publish.assert_called_once_with(
                exchange_name=self.exchange_name, persistent=self.persistent,
                routing_key=self.routing_key, message=self.unencoded_message,
                **self.basic_publish_properties)


if '__main__' == __name__:
    unittest.main()
