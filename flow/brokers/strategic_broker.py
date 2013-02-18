from flow.protocol import codec
from flow.protocol.exceptions import InvalidMessageException
from pika.spec import Basic

import blist
import collections
import logging
import os
import pika
import signal
import time

LOG = logging.getLogger(__name__)

TERMINATION_SIGNALS = [signal.SIGINT, signal.SIGTERM]


class StrategicAmqpBroker(object):
    def __init__(self, amqp_url=None, prefetch_count=None,
            acking_strategy=None):
        self.amqp_url = amqp_url
        self.prefetch_count = prefetch_count
        self.acking_strategy = acking_strategy

        self._publish_properties = pika.BasicProperties(delivery_mode=2)

        self._listeners = {}
        self.acking_strategy.register_broker(self)


    def _reset_state(self):
        LOG.debug("Resetting broker state.")
        self._last_publish_tag = 0
        self._last_receive_tag = 0

        self.acking_strategy.reset()

    def ack_if_able(self):
        ackable_tags, multiple = self.acking_strategy.pop_ackable_receive_tags()
        LOG.debug('Found %d ackable tags (multiple = %s)',
                len(ackable_tags), multiple)
        if ackable_tags:
            self.ack(ackable_tags[0], multiple=multiple)
            for tag in ackable_tags[1:]:
                self.ack(tag)

    def ack(self, receive_tag, multiple=False):
        LOG.debug('Acking message (%d), multiple = %s', receive_tag, multiple)
        self._channel.basic_ack(receive_tag, multiple=multiple)

    def reject(self, receive_tag):
        LOG.debug('Rejecting message (%d)', receive_tag)
        self._channel.basic_reject(receive_tag, requeue=False)


    def register_handler(self, handler):
        queue_name = handler.queue_name
        LOG.debug('Registering handler (%s) on queue (%s)',
                handler, queue_name)
        listener = AmqpListener(delivery_callback=handler, broker=self)
        self._listeners[queue_name] = listener


    def publish(self, exchange_name, routing_key, message):
        encoded_message = codec.encode(message)
        self.raw_publish(exchange_name, routing_key, encoded_message)

    def raw_publish(self, exchange_name, routing_key, encoded_message):
        receive_tag = self._last_receive_tag

        self._last_publish_tag += 1
        publish_tag = self._last_publish_tag
        LOG.debug("Publishing message (%d) to routing key (%s): %s",
                publish_tag, routing_key, encoded_message)

        self.acking_strategy.add_publish_tag(receive_tag=receive_tag,
                publish_tag=publish_tag)

        self._channel.basic_publish(exchange=exchange_name,
                routing_key=routing_key, body=encoded_message,
                properties=self._publish_properties)


    def connect_and_listen(self):
        self.connect()

    def connect(self):
        LOG.info('Connecting to AMQP server at: %s', self.amqp_url)
        set_termination_signal_handler(raise_handler)

        try:
            self._connection = pika.SelectConnection(
                    pika.URLParameters(self.amqp_url), self._on_connection_open)
            self._begin_ioloop()
        except KeyboardInterrupt:
            self.disconnect()

    def disconnect(self):
        LOG.info("Closing AMQP connection.")
        self._connection.close()
        self._begin_ioloop()

    def _begin_ioloop(self):
        interrupted = True
        while interrupted:
            try:
                self._connection.ioloop.start()
                interrupted = False
            except IOError:
                LOG.warning('IO interrupted, continuing')

    def reconnect(self):
        self.disconnect()
        self.connect()


    def _on_connection_open(self, connection):
        LOG.debug("Adding connection close callback")
        connection.add_on_close_callback(self._on_connection_closed)

        connection.channel(self._on_channel_open)

    def _on_connection_closed(self, method_frame):
        LOG.error("Disconnected.  Retrying in %f seconds.",
                self.reconnect_delay)
        time.sleep(self.reconnect_delay)
        self.connect()


    def _on_channel_open(self, channel):
        self._channel = channel
        LOG.debug('Channel open')
        self._reset_state()

        if self.prefetch_count:
            self._channel.basic_qos(prefetch_count=self.prefetch_count)

        self.acking_strategy.on_channel_open(channel)

        for queue_name, listener in self._listeners.iteritems():
            LOG.debug('Beginning consumption on queue (%s)', queue_name)
            self._channel.basic_consume(listener, queue_name)

    def _on_channel_closed(self, method_frame):
        LOG.error('Channel unexpectedly closed.  Attempting to reconnect')
        self.reconnect()


    def set_last_receive_tag(self, receive_tag):
        LOG.debug('Received message (%d)', receive_tag)
        self._last_receive_tag = receive_tag
        self.acking_strategy.add_receive_tag(receive_tag)


def set_termination_signal_handler(handler):
    for sig in TERMINATION_SIGNALS:
        signal.signal(sig, handler)

def raise_handler(*args):
    LOG.info("Caught signal %s, exitting.", args)
    set_termination_signal_handler(null_handler)
    raise KeyboardInterrupt('Caught Signal')

def null_handler(*args):
    LOG.warning("Caught signal %s while trying to exit.", args)


class AmqpListener(object):
    def __init__(self, broker=None, delivery_callback=None):
        self.broker = broker
        self.delivery_callback = delivery_callback

    def __call__(self, channel, basic_deliver, properties, encoded_message):
        broker = self.broker

        delivery_tag = basic_deliver.delivery_tag
        broker.set_last_receive_tag(delivery_tag)

        try:
            message = codec.decode(encoded_message)
            self.delivery_callback(message)

            LOG.debug('Checking for ack after handler (%d)', delivery_tag)
            broker.ack_if_able()

        # KeyboardInterrupt must be passed up the stack so we can terminate
        except KeyboardInterrupt:
            raise

        except InvalidMessageException as e:
            LOG.exception('Invalid message.  Properties = %s, message = %s',
                    properties, encoded_message)
            broker.reject(delivery_tag)
        except:
            LOG.exception('Unexpected error handling message.')
            broker.reject(delivery_tag)
