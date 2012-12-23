import unittest
try:
    from unittest import mock
except:
    import mock

import json
from amqp_service.responder.base import Responder


class ResponderTest(unittest.TestCase):
    def setUp(self):
        self.queue = mock.Mock()
        self.durable_queue = mock.Mock()
        self.exchange = mock.Mock()
        self.exchange_type = mock.Mock()
        self.prefetch_count = mock.Mock()

        self.channel = mock.Mock()
        self.channel.basic_publish = mock.Mock()
        self.basic_ack = mock.Mock()
        self.basic_deliver = mock.Mock()
        self.basic_deliver.delivery_tag = mock.Mock()
        self.basic_reject = mock.Mock()
        self.properties = mock.Mock()
        self.body = '{"key": "value"}'

        self.body_data = {u'key': u'value'}

        self.responder = Responder(queue=self.queue,
                durable_queue=self.durable_queue,
                exchange=self.exchange,
                exchange_type=self.exchange_type,
                prefetch_count=self.prefetch_count)

        self.publish_properties = self.responder._properties


    def test_message_receiver_normal(self):
        self.responder.on_message = mock.Mock()
        routing_key = 'trk'
        output_message = 'message'
        self.responder.on_message.return_value = (routing_key, output_message)


        self.responder.message_receiver(self.channel,
                self.basic_deliver, self.properties, self.body)

        self.responder.on_message.assert_called_once_with_with(self.channel,
                self.basic_deliver, self.properties, self.body_data)

        self.channel.basic_publish.assert_called_once_with(
                exchange=self.exchange, body=json.dumps(output_message),
                routing_key=routing_key,
                properties=self.publish_properties)

        self.channel.basic_ack.assert_called_once_with(
                self.basic_deliver.delivery_tag)


    def test_message_receiver_exception(self):
        self.responder.message_receiver(self.channel,
                self.basic_deliver, self.properties, self.body)

        self.channel.basic_reject.assert_called_once(
                self.basic_deliver.delivery_tag_with)


    def test_on_message(self):
        self.assertRaises(RuntimeError, self.responder.on_message,
                None, None, None, None)


if '__main__' == __name__:
    unittest.main()