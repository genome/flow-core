from flow.brokers.amqp_parameters import AmqpConnectionParameters
from flow.brokers.base import BrokerBase
from injector import inject

import logging
import pika

LOG = logging.getLogger(__name__)


@inject(connection_params=AmqpConnectionParameters)
class BlockingAmqpBroker(BrokerBase):
    def connect(self):
        params = pika.ConnectionParameters(
                host=self.connection_params.hostname,
                port=self.connection_params.port,
                virtual_host=self.connection_params.virtual_host)
        self.connection = pika.BlockingConnection(params)

        self.channel = self.connection.channel()
        self.channel.confirm_delivery()

    def disconnect(self):
        self.connection.close()

    def create_bound_temporary_queue(self, exchange_name, topic, queue_name):
        self.create_temporary_queue(queue_name)

        self.channel.queue_bind(queue_name, exchange_name, topic)

    def create_temporary_queue(self, queue_name):
        self.channel.queue_declare(queue_name,
                durable=False, auto_delete=True, exclusive=True)

    def connect_and_listen(self):
        raise NotImplementedError('...')

    def register_handler(self, handler):
        raise NotImplementedError('...')

    def raw_publish(self, exchange_name, routing_key, encoded_message):
        self.channel.basic_publish(exchange_name,
                routing_key, encoded_message)

    def raw_get(self, queue_name):
        for frame, header, body in self.channel.consume(queue_name):
            self.channel.basic_ack(frame.delivery_tag)
            break

        # XXX This if statement is a workaround for a bug in pika
        # pika.adapters.blocking_connection, line 508:
        # +         messages = []
        if self.channel._generator_messages:
            self.channel.cancel()

        return body
