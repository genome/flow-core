from twisted.python.failure import Failure
from twisted.internet import defer
from flow.protocol.exceptions import InvalidMessageException

from flow.brokers.amqp_broker import AmqpBroker

import unittest
import mock


class AmqpBrokerTests(unittest.TestCase):
    def setUp(self):
        self.channel = mock.Mock()

        self.b = AmqpBroker(channel=self.channel)

    def test_publish(self):
        exchange_name = mock.Mock()
        routing_key = mock.Mock()
        message = mock.Mock()
        encoded_message = mock.Mock()
        message.encode = mock.Mock(return_value=encoded_message)

        expected_return_value = mock.Mock()
        self.channel.basic_publish = mock.Mock(
                return_value=expected_return_value)
        return_value = self.b.publish(exchange_name=exchange_name,
                routing_key=routing_key, message=message)
        self.assertIs(return_value, expected_return_value)
        self.channel.basic_publish.assert_called_once_with(
                exchange_name=exchange_name,
                routing_key=routing_key,
                encoded_message=encoded_message)

    def test_register_handler(self):
        handler = mock.Mock()
        deferred = mock.Mock()
        self.channel.connect = mock.Mock(return_value=deferred)

        return_value = self.b.register_handler(handler)
        self.assertIs(return_value, deferred)

        deferred.addCallback.assert_called_once_with(self.b._start_handler,
                handler)

    def test_private_start_handler(self):
        channel = mock.Mock()
        self.b.channel = mock.Mock()
        deferred = mock.Mock()
        channel.basic_consume = mock.Mock(return_value=deferred)
        handler = mock.Mock()
        handler.queue_name = 'fake_queue_name'

        return_value = self.b._start_handler(channel, handler=handler)
        self.assertIs(return_value, channel)

        self.b.channel.basic_consume.assert_called_once_with(queue='fake_queue_name')

    def test_private_begin_get_loop(self):
        queue = mock.Mock()
        consumer_tag = mock.Mock()
        consume_info = (queue, consumer_tag)
        handler = mock.Mock()

        self.b._get_message_from_queue = mock.Mock()

        return_value = self.b._begin_get_loop(consume_info, handler)
        self.assertIs(return_value, consume_info)
        self.b._get_message_from_queue.assert_called_once_with(
                queue=queue, handler=handler)

    def test_private_get_message_from_queue(self):
        deferred = mock.Mock(defer.Deferred)
        queue = mock.Mock()
        queue.get = mock.Mock(return_value=deferred)
        handler = mock.Mock()

        return_value = self.b._get_message_from_queue(queue, handler)
        self.assertIs(return_value, deferred)

        deferred.addCallback.assert_any_call(mock.ANY, queue, handler)
        deferred.addErrback.assert_any_call(mock.ANY, handler)

    def test_private_on_get_failed(self):
        reason = mock.Mock()
        handler = mock.Mock()
        fake_exit = mock.Mock()

        with mock.patch('flow.brokers.amqp_broker.exit_process', new=fake_exit):
            self.b._on_get_failed(reason, handler=handler)
            self.assertEqual(fake_exit.call_count, 1)

    def test_private_on_message_recieved(self):
        channel = mock.Mock()
        basic_deliver = mock.Mock()
        recieve_tag = mock.Mock()
        basic_deliver.delivery_tag = recieve_tag
        properties = mock.Mock()
        encoded_message = mock.Mock()
        get_info = (channel, basic_deliver, properties, encoded_message)

        queue = mock.Mock()
        deferred = mock.Mock()
        handler = mock.Mock(return_value=deferred)
        handler.message_class = mock.Mock()
        message = mock.Mock()
        handler.message_class.decode = mock.Mock(return_value=message)

        self.b._get_message_from_queue = mock.Mock()

        # without raising exception
        return_value = self.b._on_message_recieved(get_info, queue, handler)
        self.assertIs(return_value, get_info)

        handler.assert_called_once_with(message)
        deferred.addCallbacks.assert_called_once_with(
                self.b._ack, self.b._reject,
                callbackArgs=(recieve_tag,),
                errbackArgs=(recieve_tag,))
        self.b._get_message_from_queue.assert_called_once_with(queue, handler)

        # raising InvalidMessageException
        handler.message_class.decode.side_effect = InvalidMessageException
        failed_deferred = mock.Mock()
        fake_fail = mock.Mock(return_value=failed_deferred)
        with mock.patch('twisted.internet.defer.fail', new=fake_fail):
            return_value = self.b._on_message_recieved(get_info, queue, handler)
            self.assertIs(return_value, get_info)

            handler.assert_called__with(message)
            failed_deferred.addCallbacks.assert_called_with(
                    self.b._ack, self.b._reject,
                    callbackArgs=(recieve_tag,),
                    errbackArgs=(recieve_tag,))
            self.b._get_message_from_queue.assert_called_with(queue, handler)


    def test_private_ack(self):
        confirm_info = mock.Mock()
        recieve_tag = mock.Mock()
        self.b.channel = mock.Mock()

        return_value = self.b._ack(confirm_info, recieve_tag)
        self.assertIs(return_value, confirm_info)
        self.b.channel.basic_ack.assert_called_once_with(recieve_tag)

    def test_private_reject(self):
        reason = mock.Mock()
        recieve_tag = mock.Mock()
        self.b.channel = mock.Mock()

        return_value = self.b._reject(reason, recieve_tag)
        self.assertIs(return_value, None)
        self.b.channel.basic_reject.assert_called_once_with(recieve_tag)

