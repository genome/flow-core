import logging
import pika
import signal

from delegate_base import Delegate

LOG = logging.getLogger(__name__)

class ConnectionManager(Delegate):
    _REQUIRED_DELEGATE_METHODS = ['on_connection_open', 'on_connection_closed']

    def __init__(self, url, reconnect_sleep=60, **kwargs):
        Delegate.__init__(self, **kwargs)
        self.url = url
        self.reconnect_sleep = reconnect_sleep


    def _on_connection_open(self, connection):
        self._connection = connection
        LOG.debug("adding connection close callback")
        connection.add_on_close_callback(self._on_connection_closed)

        for delegate in self.delegates:
            try:
                delegate.on_connection_open(self)
            except:
                LOG.exception('Delegating on_connection_open to %s failed',
                        delegate)

    def _on_connection_closed(self, method_frame):
        LOG.warning("Connection closed.  method_frame: %s", method_frame)

        # if you re-declare a queue or exchange with wrong params, connection
            # will be closed -- can we inspect the frame and behave differently?
        for delegate in self.delegates:
            try:
                delegate.on_connection_closed(method_frame)
            except:
                LOG.exception('Delegation of on_connection_closed to %s failed',
                        delegate)

        LOG.info("Sleeping for %d seconds before next reconnect attempt",
                self.reconnect_sleep)
        self._connection.ioloop.add_timeout(self.reconnect_sleep, self.start)


    def channel(self, *args, **kwargs):
        self._connection.channel(*args, **kwargs)


    def start(self):
        LOG.info("Attempting to connect to AMQP broker")
        signal.signal(signal.SIGINT, raise_handler)
        signal.signal(signal.SIGTERM, raise_handler)
        self._connection = pika.SelectConnection(pika.URLParameters(self.url),
                self._on_connection_open)
        try:
            self._connection.ioloop.start()
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        signal.signal(signal.SIGINT, null_handler)
        signal.signal(signal.SIGTERM, null_handler)
        self._connection.close()
        self._connection.ioloop.start()

def raise_handler(*args):
    raise KeyboardInterrupt('Caught Signal')

def null_handler(*args):
    pass
