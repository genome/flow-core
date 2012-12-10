import logging
import pika

from functools import partial

LOG = logging.getLogger(__name__)

class ConnectionManager(object):
    def __init__(self, url, quick_reconnect_max_attempts=3,
            slow_reconnect_delay=60, slow_reconnect_max_attempts=120):
        self.url = url
        self.quick_reconnect_max_attempts = quick_reconnect_max_attempts
        self.slow_reconnect_delay = slow_reconnect_delay
        self.slow_reconnect_max_attempts = slow_reconnect_max_attempts

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
        connection.add_on_close_callback(self._on_connection_closed)

        self._quick_reconnect_attempts = 0
        self._slow_reconnect_attempts = 0

        for responder in self._responders:
            connection.channel(on_open_callback=partial(self._on_channel_open, responder))

    def _on_connection_closed(self, method_frame):
        # if you re-declare a queue or exchange with wrong params, connection
            # will be closed
        pass
        # reconnect logic goes here


    def _on_channel_open(self, responder, channel):
        # mark channel as open
        channel.add_on_close_callback(partial(self._on_channel_closed, responder))
        self._channels[responder] = channel

        channel.queue_declare(partial(self._on_queue_declare_ok, responder),
                responder.queue, durable=responder.durable_queue)
        channel.exchange_declare(partial(self._on_exchange_declare_ok, responder),
                responder.exchange, responder.exchange_type)

    def _on_channel_closed(self, responder, channel):
        if responder in self._channels:
            # mark channel as closed - is there anything else to do?
            del self._channels[responder]


    def _on_queue_declare_ok(self, responder, method_frame):
        self._queues_to_create.discard(responder.queue)
        self._start_consuming_if_connection_complete()

    def _on_exchange_declare_ok(self, responder, method_frame):
        self._exchanges_to_create.discard(responder.exchange)
        self._start_consuming_if_connection_complete()


    def _start_consuming_if_connection_complete(self):
        if self._check_status():
            self._start_consuming()

    def _check_status(self):
        return (not self._queues_to_create) and (not self._exchanges_to_create)

    def _start_consuming(self):
        for responder in self._responders:
            channel = self._channels[responder]
            channel.basic_consume(responder.message_receiver, responder.queue)

    def _on_cancel_ok(self, responder, frame):
        LOG.debug("channel closed for responder: %s", responder)
        if responder in self._channels:
            del self._channels[responder]
        if not self._channels:
            self._connection.close()

    def run(self):
        LOG.debug("connection_manager.run")
        # collect set of flags that must be met
        self._queues_to_create = set()
        self._exchanges_to_create = set()
        for responder in self._responders:
            self._queues_to_create.add(responder.queue)
            self._exchanges_to_create.add(responder.exchange)

        self._connection = pika.SelectConnection(pika.URLParameters(self.url),
                self._on_connection_open)
        self._connection.ioloop.start()

    def stop(self):
        # The event loop doesn't seem to like this..
#        for responder, channel in self._channels.iteritems():
#            print responder, channel
#            channel.basic_cancel(partial(self._on_cancel_ok, responder))
        self._connection.close()
        self._connection.ioloop.start()
