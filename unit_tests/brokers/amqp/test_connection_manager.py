import unittest
try:
    from unittest import mock
except:
    import mock

from twisted.internet import defer, reactor

from flow.brokers.amqp.connection_manager import (ConnectionManager,
        ConnectionParams)
from flow.brokers.amqp.connection_manager import (DISCONNECTED, CONNECTING,
        CONNECTED)
from flow.exit_codes import (EXECUTE_SYSTEM_FAILURE,
        EXECUTE_SERVICE_UNAVAILABLE)

connection_params = ConnectionParams(
        hostname=object(),
        port=object(),
        virtual_host=object(),
        retry_delay=0.01,
        connection_attempts=3,
        prefetch_count=1,
        heartbeat_interval=600,
)

class ConnectionManagerTests(unittest.TestCase):
    def setUp(self):
        self.fake_fn = mock.Mock()
        with mock.patch('twisted.internet.reactor.addSystemEventTrigger',
                new=self.fake_fn):
            self.cm = ConnectionManager(connection_params=connection_params)

    def test_init(self):
        self.fake_fn.assert_called_once_with('before', 'shutdown',
                self.cm._disconnect)
        self.assertEqual(self.cm.state, DISCONNECTED)

    def test_connect(self):
        self.cm._attempt_to_connect = mock.Mock()

        returned_deferred = self.cm.connect()
        self.assertTrue(isinstance(returned_deferred, defer.Deferred))
        self.assertFalse(returned_deferred.called)
        self.assertEqual(self.cm._attempt_to_connect.call_count, 1)

        self.cm.state = CONNECTED
        second_deferred = self.cm.connect()
        self.assertTrue(second_deferred is returned_deferred)
        self.assertFalse(returned_deferred.called) # still not called
        # wasn't attempting to connect again
        self.assertEqual(self.cm._attempt_to_connect.call_count, 1)

        self.cm.state = CONNECTING
        third_deferred = self.cm.connect()
        self.assertTrue(third_deferred is returned_deferred)
        self.assertFalse(returned_deferred.called) # still not called
        # wasn't attempting to connect again
        self.assertEqual(self.cm._attempt_to_connect.call_count, 1)

    def test_private_disconnect(self):
        # can call without being connected just fine.
        self.cm._disconnect()

        self.cm._connection = mock.Mock()
        self.cm._connection.transport = mock.Mock()
        self.cm._connection.transport.loseConnection = mock.Mock()
        self.cm._disconnect()
        self.assertTrue(self.cm._connection.transport.loseConnection.called)

    def _help_with_attempt_to_connect(self):
        self._connect_deferreds = []
        def make_deferred(*args):
            deferred = defer.Deferred()
            self._connect_deferreds.append(deferred)
            return deferred

        fake_pika_connection = mock.Mock()
        fake_pika_connection.ready = mock.Mock()
        fake_pika_connection.ready.addCallback = mock.Mock()
        fake_pika_connection.add_on_close_callback = mock.Mock()
        fake_pika_connection.connectTCP =  make_deferred
        self.fake_pika_connection = fake_pika_connection

        self.cm._create_pika_connection = mock.Mock()
        self.cm._create_pika_connection.return_value = fake_pika_connection
        return fake_pika_connection

    def test_private_attempt_to_connect(self):
        self._help_with_attempt_to_connect()

        connect_deferred = self.cm.connect()

        self.assertEqual(self.cm.state, CONNECTING)
        self.assertEqual(self.cm._connection_attempts, 1)
        self.assertEqual(len(self._connect_deferreds), 1)
        self.cm._create_pika_connection.assert_called_once_with(
                connection_params)

    def test_private_on_connectTCP(self):
        connection = mock.Mock()
        return_value = self.cm._on_connectTCP(connection)
        self.assertIs(return_value, connection)

        self.assertTrue(self.cm._connection is connection)

        self.cm._connection.ready.addCallback.assert_called_once_with(
                self.cm._on_ready)

        self.cm._connection.add_on_close_callback.assert_called_once_with(
                self.cm._on_pika_connection_closed)


    def test_private_create_pika_connection(self):
        fake_pika_cp = mock.Mock()
        with mock.patch('pika.ConnectionParameters', new=fake_pika_cp):
            fake_client_creator = mock.Mock()
            expected_return_value = mock.Mock()
            fake_client_creator.return_value = expected_return_value
            with mock.patch('twisted.internet.protocol.ClientCreator',
                    new=fake_client_creator):
                return_value = self.cm._create_pika_connection(
                        connection_params)
                self.assertTrue(return_value is expected_return_value)
                self.assertEqual(fake_pika_cp.call_count, 1)
                self.assertEqual(fake_client_creator.call_count, 1)

    def test_private_on_ready(self):
        self.cm._connect_deferred = mock.Mock()
        self.cm._connect_deferred.callback = mock.Mock()
        ch_d = defer.Deferred()
        bq_d = defer.Deferred()

        connection = mock.Mock()
        connection.channel = mock.Mock()
        connection.channel.return_value = ch_d

        deferred = self.cm._on_ready(connection)
        self.assertTrue(self.cm._channel is None)
        self.assertEquals(self.cm.state, DISCONNECTED)

        channel = mock.Mock()
        channel.basic_qos = mock.Mock()
        channel.basic_qos.return_value = bq_d
        ch_d.callback(channel) # releases first yield
        self.assertTrue(self.cm._channel is channel)
        self.assertEquals(self.cm.state, DISCONNECTED)
        channel.basic_qos.assert_called_once_with(
                prefetch_count=connection_params.prefetch_count)

        bq_d.callback(None) # releases second yield
        self.assertEquals(self.cm.state, CONNECTED)
        self.cm._connect_deferred.callback.assert_called_once_with(channel)


    def test_private_on_connectTCP_failed(self):
        self.cm._connection_attempts = 1
        self.cm.state = CONNECTING
        self.cm._connect_deferred = mock.Mock()
        self.cm._connect_deferred.errback = mock.Mock()

        callLater = mock.Mock()
        reason = object()
        with mock.patch('twisted.internet.reactor.callLater',
                new=callLater):
            self.cm._on_connectTCP_failed(reason)
            callLater.assert_called_once_with(connection_params.retry_delay,
                    self.cm._attempt_to_connect)
            self.assertEquals(self.cm.state, CONNECTING)

        # reached reconnect limit
        self.cm._connection_attempts = 3
        fake_exit = mock.Mock()
        with mock.patch('os._exit', new=fake_exit):
            self.cm._on_connectTCP_failed(reason)
            self.assertEquals(self.cm.state, DISCONNECTED)
            fake_exit.assert_called_once_with(EXECUTE_SERVICE_UNAVAILABLE)


    def test_private_on_pika_connection_closed(self):
        self.cm.state = CONNECTING

        fake_exit = mock.Mock()
        with mock.patch('os._exit', new=fake_exit):
            self.cm._on_pika_connection_closed(None,
                    reply_code='test_code', reply_text='test_text')

            self.assertIs(self.cm.state, DISCONNECTED)
            fake_exit.assert_called_once_with(EXECUTE_SYSTEM_FAILURE)


    def test_retry(self):
        self._connect_deferreds = []
        def make_deferred(*args):
            deferred = defer.Deferred()
            self._connect_deferreds.append(deferred)
            return deferred

        fake_pika_connection = mock.Mock()
        fake_pika_connection.ready = mock.Mock()
        fake_pika_connection.ready.addCallback = mock.Mock()
        fake_pika_connection.add_on_close_callback = mock.Mock()
        fake_pika_connection.connectTCP =  make_deferred

        self.cm._create_pika_connection = mock.Mock()
        self.cm._create_pika_connection.return_value = fake_pika_connection

        connect_deferred = self.cm.connect()
        self.assertEqual(len(self._connect_deferreds), 1)

        # only one connection attempt at a time.
        same_connect_deferred = self.cm.connect()
        self.assertTrue(connect_deferred is same_connect_deferred)
        self.assertEqual(len(self._connect_deferreds), 1)
        self.assertFalse(connect_deferred.called)

        def call_now(time, fn, *args, **kwargs):
            fn(*args, **kwargs)
        with mock.patch('twisted.internet.reactor.callLater',
                new=call_now):

            self._connect_deferreds[0].errback(RuntimeError('bad'))
            self.assertEqual(len(self._connect_deferreds), 2)
            self.assertFalse(connect_deferred.called)

            self._connect_deferreds[1].errback(RuntimeError('bad'))
            self.assertEqual(len(self._connect_deferreds), 3)
            self.assertFalse(connect_deferred.called)

            # reached retry limit
            fake_exit = mock.Mock()
            with mock.patch('os._exit', new=fake_exit):
                errback = mock.Mock()

                error = RuntimeError('bad')
                self._connect_deferreds[2].errback(error)
                self.assertEqual(len(self._connect_deferreds), 3)
                fake_exit.assert_called_once_with(EXECUTE_SERVICE_UNAVAILABLE)
