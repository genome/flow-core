from flow import exit_codes
from flow.configuration.settings.injector import setting
from flow.util.exit import exit
from injector import inject
from pika.adapters import twisted_connection
from twisted.internet import reactor, defer, protocol
from twisted.internet.error import ReactorNotRunning

import logging
import pika
import os


LOG = logging.getLogger(__name__)

@inject(
    hostname=setting('amqp.hostname'),
    port=setting('amqp.port'),
    virtual_host=setting('amqp.vhost'),
    retry_delay=setting('amqp.retry_delay'),
    connection_attempts=setting('amqp.connection_attempts'),
    prefetch_count=setting('amqp.prefetch_count'),
)
class ConnectionParams(object): pass

DISCONNECTED = 'disconnected'
CONNECTING = 'connecting'
CONNECTED = 'connected'

@inject(connection_params=ConnectionParams)
class ConnectionManager(object):
    def __init__(self):
        reactor.addSystemEventTrigger('before', 'shutdown', self._disconnect)

        self.state = DISCONNECTED
        self._channel = None
        self._connection = None

        self._connection_attempts = 0
        self._connect_deferred = None

    def connect(self):
        """
        Returns a deferred that will callback with a pika channel object once
        the connection to AMQP has been established.  os._exit will be called
        in the event that no connection could be made or if the connection
        is lost after it has been established.
        """
        if self.state is DISCONNECTED:
            self._connect_deferred = defer.Deferred()
            self._attempt_to_connect()
        else:
            if self.state is CONNECTED:
                LOG.debug("Already Connected to AMQP")
            else:
                LOG.debug('Connection to AMQP is already in progress')
        return self._connect_deferred

    def _attempt_to_connect(self):
        self.state = CONNECTING
        self._connection_attempts += 1

        LOG.debug('Attempting to establish connection to host: %s '
                'on port: %s', self.connection_params.hostname,
                self.connection_params.port)

        self._connection = self._create_pika_connection(self.connection_params)
        self._connection.ready.addCallback(self._on_ready)
        self._connection.add_on_close_callback(self._on_pika_connection_closed)

        deferred = self._connection.connectTCP(self.connection_params.hostname,
                self.connection_params.port)
        deferred.addErrback(self._on_connectTCP_failed)

    @staticmethod
    def _create_pika_connection(connection_params):
        pika_params = pika.ConnectionParameters(
                host=connection_params.hostname,
                port=connection_params.port,
                virtual_host=connection_params.virtual_host)
        connection = protocol.ClientCreator(reactor,
                twisted_connection.TwistedProtocolConnection,
                pika_params)
        return connection

    @defer.inlineCallbacks
    def _on_ready(self, connection):
        LOG.debug('Established connection to AMQP')
        self._channel = yield connection.channel()
        LOG.debug('Channel is open')

        if self.connection_params.prefetch_count:
            yield self._channel.basic_qos(
                    prefetch_count=self.connection_params.prefetch_count)

        self.state = CONNECTED
        self._connect_deferred.callback(self._channel)

    def _on_connectTCP_failed(self, reason):
        LOG.warning("Attempt %d to connect to AMQP server failed: %s",
                self._connection_attempts, reason)

        max_attempts = self.connection_params.connection_attempts
        if self._connection_attempts >= max_attempts:
            self.state = DISCONNECTED
            LOG.critical('Maximum number of connection attempts (%d) '
                    'reached... shutting down', max_attempts)
            exit(exit_codes.EXECUTE_SERVICE_UNAVAILABLE)
        else:
            LOG.info("Attempting to reconnect to the AMQP "
                    "server in %s seconds", self.connection_params.retry_delay)
            reactor.callLater(self.connection_params.retry_delay,
                    self._attempt_to_connect)

    def _on_pika_connection_closed(self, connection, reply_code, reply_text):
        LOG.info('Connection closed with code %s: %s', reply_code, reply_text)
        self.state = DISCONNECTED
        exit(exit_codes.EXECUTE_SYSTEM_FAILURE)

    def _disconnect(self):
        LOG.info("Closing AMQP connection")
        if hasattr(self._connection, 'transport'):
            self._connection.transport.loseConnection()
