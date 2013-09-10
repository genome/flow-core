from flow.configuration.settings.injector import setting
from flow.util.exit import exit_process
from injector import inject
from pika.adapters import twisted_connection
from twisted.internet import reactor, defer, protocol
from flow.exit_codes import (EXECUTE_SYSTEM_FAILURE,
        EXECUTE_ERROR,
        EXECUTE_SERVICE_UNAVAILABLE)
from flow.util.defer import add_callback_and_default_errback

import logging
import pika


LOG = logging.getLogger(__name__)

@inject(
    hostname=setting('amqp.hostname'),
    port=setting('amqp.port'),
    virtual_host=setting('amqp.vhost'),
    retry_delay=setting('amqp.retry_delay'),
    connection_attempts=setting('amqp.connection_attempts'),
    prefetch_count=setting('amqp.prefetch_count'),
    heartbeat_interval=setting('amqp.heartbeat_interval'),
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

        pika_connection = self._create_pika_connection(self.connection_params)

        deferred = pika_connection.connectTCP(self.connection_params.hostname,
                self.connection_params.port)
        deferred.addCallbacks(self._on_connectTCP, self._on_connectTCP_failed)
        deferred.addErrback(self._exit)

    def _on_connectTCP(self, connection):
        self._connection = connection
        add_callback_and_default_errback(connection.ready, self._on_ready)
        connection.add_on_close_callback(self._on_pika_connection_closed)
        return connection

    @staticmethod
    def _create_pika_connection(connection_params):
        pika_params = pika.ConnectionParameters(
                host=connection_params.hostname,
                port=connection_params.port,
                virtual_host=connection_params.virtual_host,
                heartbeat_interval=connection_params.heartbeat_interval,
                )
        connection = protocol.ClientCreator(reactor,
                twisted_connection.TwistedProtocolConnection,
                pika_params)
        return connection

    def _on_ready(self, connection):
        LOG.debug('Established connection to AMQP')
        channel_deferred = connection.channel()
        channel_deferred.addCallback(self._set_channel_and_qos)
        channel_deferred.addErrback(self._exit,
                msg='Unexpected error in getting channel')
        return channel_deferred

    def _set_channel_and_qos(self, channel):
        self._channel = channel
        LOG.debug('Channel is open')

        if self.connection_params.prefetch_count:
            qos_deferred = self._channel.basic_qos(
                    prefetch_count=self.connection_params.prefetch_count)
            qos_deferred.addCallback(self._finish_on_ready)
            qos_deferred.addErrback(self._exit,
                    msg='Unexpected error setting qos')
        else:
            self._finish_on_ready(None)
        return channel

    def _exit(self, error, msg="Unexpected error"):
        LOG.critical("%s\n%s", msg, error.getTraceback())
        exit_process(EXECUTE_ERROR)

    def _finish_on_ready(self, _callback):
        self.state = CONNECTED
        self._connect_deferred.callback(self._channel)
        return _callback

    def _on_connectTCP_failed(self, reason):
        LOG.warning("Attempt %d to connect to AMQP server failed: %s",
                self._connection_attempts, reason)

        max_attempts = self.connection_params.connection_attempts
        if self._connection_attempts >= max_attempts:
            self.state = DISCONNECTED
            LOG.critical('Maximum number of connection attempts (%d) '
                    'reached... shutting down', max_attempts)
            exit_process(EXECUTE_SERVICE_UNAVAILABLE)
        else:
            LOG.info("Attempting to reconnect to the AMQP "
                    "server in %s seconds", self.connection_params.retry_delay)
            reactor.callLater(self.connection_params.retry_delay,
                    self._attempt_to_connect)

    def _on_pika_connection_closed(self, connection, reply_code, reply_text):
        LOG.info('Connection closed with code %s: %s', reply_code, reply_text)
        self.state = DISCONNECTED
        exit_process(EXECUTE_SYSTEM_FAILURE)

    def _disconnect(self):
        LOG.info("Closing AMQP connection")
        if hasattr(self._connection, 'transport'):
            self._connection.transport.loseConnection()
