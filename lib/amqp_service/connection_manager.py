import logging
import pika

from functools import partial

LOG = logging.getLogger(__name__)

class ConnectionManager(object):
    def __init__(self, url, prefetch_count=1, quick_reconnect_max_attempts=3,
            slow_reconnect_delay=60, slow_reconnect_max_attempts=120):
        self.url = url
        self.quick_reconnect_max_attempts = quick_reconnect_max_attempts
        self.slow_reconnect_delay = slow_reconnect_delay
        self.slow_reconnect_max_attempts = slow_reconnect_max_attempts

        self.prefetch_count = prefetch_count

        self._connection = None
        self._quick_reconnect_attempts = 0
        self._slow_reconnect_attempts = 0

        self._responders = []
        self._queues_to_create = set()
        self._exchanges_to_create = set()
        self._channels = {}


    def register_responder(self, responder):
        self._responders.append(responder)


    def _on_connection_open(self, connection):
        LOG.debug("adding connection close callback")
        connection.add_on_close_callback(self._on_connection_closed)

        for responder in self._responders:
            LOG.debug("Adding channel for responder %s", responder)
            connection.channel(partial(self._on_channel_open, responder))

    def _on_connection_closed(self, method_frame):
        # if you re-declare a queue or exchange with wrong params, connection
            # will be closed
        LOG.debug("Connection closed.  method_frame: %s", method_frame)
        # reconnect logic goes here


    def _on_channel_open(self, responder, channel):
        # mark channel as open
        LOG.debug("adding close callback on channel %s for responder %s",
                channel, responder)
        channel.add_on_close_callback(
                partial(self._on_channel_closed, responder))
        self._channels[responder] = channel

        LOG.debug('Setting prefetch count for channel %s to %d',
                channel, self.prefetch_count)
        channel.basic_qos(prefetch_count=self.prefetch_count)

        LOG.debug("Declaring queue %s for responder %s",
                responder.queue, responder)
        channel.queue_declare(partial(self._on_queue_declare_ok, responder),
                responder.queue, durable=responder.durable_queue)
        LOG.debug("Declaring exchange %s for responder %s",
                responder.exchange, responder)
        arguments = {}
        if responder.alternate_exchange:
            arguments['alternate-exchange'] = responder.alternate_exchange
        channel.exchange_declare(
                partial(self._on_exchange_declare_ok, responder),
                responder.exchange, responder.exchange_type, durable=True,
                arguments=arguments)

    def _on_channel_closed(self, responder, channel):
        LOG.error("Channel %s closed for responder %s", channel, responder)
        if responder in self._channels:
            LOG.debug("Removing channel %s from tracking")
            # mark channel as closed - is there anything else to do?
            del self._channels[responder]


    def _on_queue_declare_ok(self, responder, method_frame):
        LOG.debug("Queue %s successfully declared for responder %s",
                responder.queue, responder)
        self._queues_to_create.discard(responder.queue)
        self._start_consuming_if_connection_complete()

    def _on_exchange_declare_ok(self, responder, method_frame):
        LOG.debug("Exchange %s successfully declared for responder %s",
                responder.exchange, responder)
        self._exchanges_to_create.discard(responder.exchange)
        self._start_consuming_if_connection_complete()


    def _start_consuming_if_connection_complete(self):
        if self._check_status():
            self._start_consuming()

    def _check_status(self):
        return (not self._queues_to_create) and (not self._exchanges_to_create)

    def _start_consuming(self):
        LOG.debug("Beginning to consume for %d responders",
                len(self._responders))
        for responder in self._responders:
            channel = self._channels[responder]
            LOG.debug("Consuming on queue %s using channel %s for responder %s",
                    responder.queue, channel, responder)
            channel.basic_consume(responder.message_receiver, responder.queue)


    def run(self):
        LOG.debug("collecting queues and exchanges to create")
        self._queues_to_create = set()
        self._exchanges_to_create = set()
        for responder in self._responders:
            self._queues_to_create.add(responder.queue)
            self._exchanges_to_create.add(responder.exchange)

        LOG.debug("connecting to AMQP host")
        self._connection = pika.SelectConnection(pika.URLParameters(self.url),
                self._on_connection_open)
        self._connection.ioloop.start()

    def stop(self):
        self._connection.close()
        self._connection.ioloop.start()
