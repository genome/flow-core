import logging
import pika

from functools import partial

LOG = logging.getLogger(__name__)

class ConnectionManager(object):
    def __init__(self, url, actors=[], reconnect_sleep=60):
        self.url = url
        self.actors = actors

        self.reconnect_sleep = reconnect_sleep

    def _on_connection_open(self, connection):
        LOG.debug("adding connection close callback")
        connection.add_on_close_callback(self._on_connection_closed)

        for actor in self.actors:
            actor.on_connection_open(connection)

    def _on_connection_closed(self, method_frame):
        # if you re-declare a queue or exchange with wrong params, connection
            # will be closed -- can we inspect the frame and log differently?
        for actor in self.actors:
            actor.on_connection_closed(method_frame)

        LOG.warning("Connection closed.  method_frame: %s", method_frame)
        LOG.info("Sleeping for %d seconds before next reconnect attempt",
                self.reconnect_sleep)

        self.run()

    def run(self):
        LOG.info("Attempting to connect to AMQP broker")
        self._connection = pika.SelectConnection(pika.URLParameters(self.url),
                self._on_connection_open)
        self._connection.ioloop.start()

    def stop(self):
        self._connection.close()
        self._connection.ioloop.start()
