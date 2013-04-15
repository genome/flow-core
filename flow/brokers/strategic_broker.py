from flow.brokers.acking_strategies import PublisherConfirmation
from flow.brokers.amqp_parameters import AmqpConnectionParameters
from flow.brokers.base import BrokerBase
from flow.protocol.exceptions import InvalidMessageException
from flow.util import stats
from injector import inject, Setting
from pika.adapters import twisted_connection
from twisted.internet import reactor, protocol, defer
from twisted.internet.error import ReactorNotRunning

import logging
import pika


LOG = logging.getLogger(__name__)

@inject(connection_params=AmqpConnectionParameters,
        prefetch_count=Setting('amqp.prefetch_count'),
        acking_strategy=PublisherConfirmation)
class StrategicAmqpBroker(BrokerBase):
    def __init__(self):
        self._publish_properties = pika.BasicProperties(delivery_mode=2)

        self._consumer_tags = []
        self._listeners = {}
        self.acking_strategy.register_broker(self)

        self._last_publish_tag = 0
        self._last_receive_tag = 0

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
        self.acking_strategy.remove_receive_tag(receive_tag)
        self._channel.basic_reject(receive_tag, requeue=False)

    def register_handler(self, handler):
        queue_name = handler.queue_name
        message_class = handler.message_class

        LOG.debug('Registering handler (%s) listening for (%s) on queue (%s)',
                handler, message_class.__name__, queue_name)

        listener = AmqpListener(delivery_callback=handler,
                message_class=message_class, broker=self)
        self._listeners[queue_name] = listener

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
        params = pika.ConnectionParameters(
                host=self.connection_params.hostname,
                port=self.connection_params.port,
                virtual_host=self.connection_params.virtual_host)

        self._connection = protocol.ClientCreator(reactor,
                twisted_connection.TwistedProtocolConnection,
                params)

        LOG.debug('Attempting to establish connection to host: %s on port: %s',
                params.host, params.port)
        deferred = self._connection.connectTCP(params.host, params.port)
        deferred.addCallback(self._on_connected)

        reactor.addSystemEventTrigger('before', 'shutdown', self.disconnect)
        reactor.run()

    def disconnect(self):
        LOG.info("Closing AMQP connection.")
        # XXX bug: _connection does not always have a transport member
        self._connection.transport.loseConnection()

    def _on_connected(self, connection):
        LOG.debug('Established connection to AMQP')
        connection.ready.addCallback(self._on_ready)
        self._connection = connection

    @defer.inlineCallbacks
    def _on_ready(self, connection):
        self._channel = yield connection.channel()
        LOG.debug('Channel open')

        if self.prefetch_count:
            self._channel.basic_qos(prefetch_count=self.prefetch_count)

        self.acking_strategy.on_channel_open(self._channel)

        for queue_name, listener in self._listeners.iteritems():
            (queue, consumer_tag) = yield self._channel.basic_consume(
                    queue=queue_name)
            self._consumer_tags.append(consumer_tag)
            listener.register_queue(queue)
            LOG.debug('Beginning consumption on queue (%s)', queue_name)

    def set_last_receive_tag(self, receive_tag):
        LOG.debug('Received message (%d)', receive_tag)
        self._last_receive_tag = receive_tag
        self.acking_strategy.add_receive_tag(receive_tag)


class AmqpListener(object):
    def __init__(self, broker=None, message_class=None, delivery_callback=None):
        self.broker = broker
        self.message_class = message_class
        self.delivery_callback = delivery_callback

        self.message_stats_tag = 'messages.receive.%s' % message_class.__name__

    def register_queue(self, queue):
        LOG.debug('Registered %r to AmqpListener %r', queue, self)
        self._queue = queue
        self._get()

    def _get(self):
        deferred = self._queue.get()
        deferred.addCallback(self.listen_and_get)
        deferred.addErrback(self._on_connection_lost)
        return deferred

    def listen_and_get(self, args):
        if self._queue is None:
            raise RuntimeError("Called listen_and_get before queue was"
                    " registered!" % self)

        self.listen(*args)
        # ensure we start listening for next item on queue
        self._get()

    @staticmethod
    def _on_connection_lost(reason):
        try:
            reactor.stop()
            LOG.critical('Disconnected from AMQP server')
        except ReactorNotRunning:
            # multiple AmqpListeners sometimes get disconnected at once
            pass

    def listen(self, channel, basic_deliver, properties, encoded_message):
        timer = stats.create_timer(self.message_stats_tag)
        timer.start()
        broker = self.broker

        delivery_tag = basic_deliver.delivery_tag
        broker.set_last_receive_tag(delivery_tag)
        LOG.debug('Received message (%d), properties = %s',
                delivery_tag, properties)

        timer.split('setup')
        try:
            message = self.message_class.decode(encoded_message)
            timer.split('decode')
            self.delivery_callback(message)
            timer.split('handle')

            LOG.debug('Checking for ack after handler (%d)', delivery_tag)
            broker.ack_if_able()
            timer.split('ack')
        except InvalidMessageException:
            LOG.exception('Invalid message.  Properties = %s, message = %s',
                    properties, encoded_message)
            broker.reject(delivery_tag)
            timer.split('reject')
        except:
            LOG.exception('Unexpected error handling message.')
            broker.reject(delivery_tag)
            timer.split('reject')
        finally:
            timer.stop()
