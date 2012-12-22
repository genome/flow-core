import logging
import pika
import time

from functools import partial

LOG = logging.getLogger(__name__)

class ConnectionManager(object):
    def __init__(self, url, delegates=[], reconnect_sleep=60):
        self.url = url
        self.delegates = delegates

        self.reconnect_sleep = reconnect_sleep

    def _on_connection_open(self, connection):
        LOG.debug("adding connection close callback")
        connection.add_on_close_callback(self._on_connection_closed)

        for delegate in self.delegates:
            try:
                delegate.on_connection_open(connection)
            except:
                LOG.exception('Delegating on_connection_open to %s failed',
                        delegate)

    def _on_connection_closed(self, method_frame):
        # if you re-declare a queue or exchange with wrong params, connection
            # will be closed -- can we inspect the frame and behave differently?
        for delegate in self.delegates:
            try:
                delegate.on_connection_closed(method_frame)
            except:
                LOG.exception('Delegating on_connection_closed to %s failed',
                        delegate)

        LOG.warning("Connection closed.  method_frame: %s", method_frame)
        LOG.info("Sleeping for %d seconds before next reconnect attempt",
                self.reconnect_sleep)
        time.sleep(self.reconnect_sleep)

        self.start()

    def start(self):
        LOG.info("Attempting to connect to AMQP broker")
        self._connection = pika.SelectConnection(pika.URLParameters(self.url),
                self._on_connection_open)
        self._connection.ioloop.start()

    def stop(self):
        self._connection.close()
        self._connection.ioloop.start()
