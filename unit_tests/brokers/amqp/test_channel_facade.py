import unittest
try:
    from unittest import mock
except:
    import mock

from flow.brokers.amqp.channel_facade import ChannelFacade
from twisted.internet import defer

class ChannelFacadeTests(unittest.TestCase):
    def setUp(self):
        self.cm = mock.Mock()
        self.cm.connect = mock.Mock()
        self.connect_deferred = defer.Deferred()
        self.cm.connect.return_value = self.connect_deferred
        self.cf = ChannelFacade(connection_manager=self.cm)

    def test_init(self):
        self.assertIs(self.cf._pika_channel, None)
        self.assertIs(self.cf._publisher_confirm_manager, None)
        self.assertEqual(self.cf._last_publish_tag, 0)

    def test_connect(self):
        self.cf._on_connected = mock.Mock()

        connect_deferred = self.cf.connect()
        self.assertIs(connect_deferred, self.connect_deferred)
        self.assertEqual(self.cf._on_connected.call_count, 0)

        channel = mock.Mock()
        connect_deferred.callback(channel)
        self.cf._on_connected.assert_called_once_with(channel)

        # another call to connect gets same deferred but does not
        # add _on_connected as a callback
        same_connect_deferred = self.cf.connect()
        self.assertIs(same_connect_deferred, connect_deferred)
        self.cf._on_connected.assert_called_once_with(channel)

    def test_private_on_connected(self):
        fake_pcm = mock.Mock()
        with mock.patch(
                'flow.brokers.amqp.channel_facade.PublisherConfirmManager',
                new=fake_pcm):
            fake_pika_channel = mock.Mock()
            self.cf._on_connected(fake_pika_channel)

            fake_pcm.assert_called_once_with(fake_pika_channel)
            self.assertIs(self.cf._pika_channel, fake_pika_channel)

    def test_bind_queue(self):
        self.cf._connect_and_do = mock.Mock()
        queue_name = mock.Mock()
        exchange_name = mock.Mock()
        topic = mock.Mock()
        properties = {'a':mock.Mock(), 'b':mock.Mock()}
        self.cf.bind_queue(queue_name=queue_name,
                exchange_name=exchange_name,
                topic=topic,
                **properties)

        self.cf._connect_and_do.assert_called_once_with('queue_bind',
                queue=queue_name,
                exchange=exchange_name,
                routing_key=topic,
                **properties)

    def test_declare_queue(self):
        self.cf._connect_and_do = mock.Mock()
        queue_name = mock.Mock()
        durable = mock.Mock()
        properties = {'a':mock.Mock(), 'b':mock.Mock()}
        self.cf.declare_queue(queue_name=queue_name,
                durable=durable,
                **properties)

        self.cf._connect_and_do.assert_called_once_with('queue_declare',
                queue=queue_name,
                durable=durable,
                **properties)

    def test_declare_exchange(self):
        self.cf._connect_and_do = mock.Mock()
        exchange_name = mock.Mock()
        exchange_type = mock.Mock()
        durable = mock.Mock()
        properties = {'a':mock.Mock(), 'b':mock.Mock()}
        self.cf.declare_exchange(exchange_name=exchange_name,
                exchange_type=exchange_type,
                durable=durable,
                **properties)

        self.cf._connect_and_do.assert_called_once_with('exchange_declare',
                exchange=exchange_name,
                exchange_type=exchange_type,
                durable=durable,
                **properties)

    def test_basic_publish(self):
        self.cf._connect_and_do = mock.Mock()
        self.cf._publisher_confirm_manager = mock.Mock()
        exchange_name = mock.Mock()
        routing_key = mock.Mock()
        encoded_message = mock.Mock()

        FakeDeferred = mock.Mock()
        fake_deferred = mock.Mock()
        FakeDeferred.return_value = fake_deferred
        with mock.patch('twisted.internet.defer.Deferred', new=FakeDeferred):
            return_value = self.cf.basic_publish(exchange_name=exchange_name,
                    routing_key=routing_key,
                    encoded_message=encoded_message)
            self.assertIs(return_value, fake_deferred)

            self.cf._connect_and_do.assert_called_once_with('basic_publish',
                    exchange=exchange_name,
                    routing_key=routing_key,
                    body=encoded_message,
                    properties=self.cf._publish_properties)
            self.assertEqual(self.cf._last_publish_tag, 1)
            self.cf._publisher_confirm_manager.add_confirm_deferred.assert_called_once_with(
                    publish_tag=1, deferred=fake_deferred)


    def test_basic_ack(self):
        self.cf._pika_channel = mock.Mock()
        self.cf._pika_channel.basic_ack = mock.Mock()
        expected_return_value = mock.Mock()
        self.cf._pika_channel.basic_ack.return_value = expected_return_value
        recieve_tag = mock.Mock()
        return_value = self.cf.basic_ack(recieve_tag)

        self.assertIs(return_value, expected_return_value)
        self.cf._pika_channel.basic_ack.assert_called_once_with(recieve_tag)

    def test_basic_reject(self):
        self.cf._pika_channel = mock.Mock()
        self.cf._pika_channel.basic_reject = mock.Mock()
        expected_return_value = mock.Mock()
        self.cf._pika_channel.basic_reject.return_value = expected_return_value
        recieve_tag = mock.Mock()
        requeue = mock.Mock()
        return_value = self.cf.basic_reject(recieve_tag=recieve_tag,
                requeue=requeue)

        self.assertIs(return_value, expected_return_value)
        self.cf._pika_channel.basic_reject.assert_called_once_with(recieve_tag,
                requeue=requeue)

    def test_private_connect_and_do(self):
        fn_name = mock.Mock()
        arg1 = mock.Mock()
        arg2 = mock.Mock()
        kwarg1 = mock.Mock()
        kwarg2 = mock.Mock()

        # for _pika_channel = None
        fake_deferred = mock.Mock()
        FakeDeferred = mock.Mock(return_value=fake_deferred)
        with mock.patch('twisted.internet.defer.Deferred', new=FakeDeferred):
            self.cf._pika_channel = None

            connect_deferred = mock.Mock()
            connect_deferred.addCallback = mock.Mock()
            self.cf.connect = mock.Mock(return_value=connect_deferred)

            return_value = self.cf._connect_and_do(fn_name, arg1, arg2,
                    kwarg1=kwarg1, kwarg2=kwarg2)
            self.assertIs(return_value, fake_deferred)
            self.assertEqual(self.cf.connect.call_count, 1)
            connect_deferred.addCallback.assert_called_once_with(
                    self.cf._do_on_channel,
                    fn_name=fn_name, args=(arg1, arg2),
                    kwargs={'kwarg1':kwarg1, 'kwarg2':kwarg2},
                    deferred=fake_deferred)

        # for _pika_channel != None
        fn_name = 'fake_fn_name'
        fake_pika_channel = mock.Mock()
        expected_return_value = mock.Mock()
        fake_pika_channel.fake_fn_name = mock.Mock(
                return_value=expected_return_value)

        self.cf._pika_channel = fake_pika_channel
        return_value = self.cf._connect_and_do(fn_name, arg1, arg2,
                kwarg1=kwarg1, kwarg2=kwarg2)
        self.assertIs(return_value, expected_return_value)
        fake_pika_channel.fake_fn_name.assert_called_once_with(arg1, arg2,
                kwarg1=kwarg1, kwarg2=kwarg2)

    def test_static_do_on_channel(self):
        arg1 = mock.Mock()
        arg2 = mock.Mock()
        kwarg1 = mock.Mock()
        kwarg2 = mock.Mock()

        fn_name = 'fake_fn_name'
        fake_pika_channel = mock.Mock()
        this_things_deferred = mock.Mock()
        fake_pika_channel.fake_fn_name = mock.Mock(
                return_value=this_things_deferred)

        deferred = mock.Mock()
        return_value = self.cf._do_on_channel(fake_pika_channel, fn_name,
                args=(arg1, arg2),
                kwargs={'kwarg1':kwarg1, 'kwarg2':kwarg2},
                deferred=deferred)
        self.assertIs(return_value, fake_pika_channel)

        fake_pika_channel.fake_fn_name.assert_called_once_with(arg1, arg2,
                kwarg1=kwarg1, kwarg2=kwarg2)
        this_things_deferred.chainDeferred.assert_called_once_with(deferred)

