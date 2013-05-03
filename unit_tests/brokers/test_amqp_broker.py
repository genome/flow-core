import unittest
try:
    from unittest import mock
except:
    import mock

from twisted.python.failure import Failure
from twisted.internet import defer

from flow.brokers.amqp_broker import AmqpBroker
from flow.brokers import amqp_broker

class AmqpBrokerTest(unittest.TestCase):
    def setUp(self):
        self.broker = AmqpBroker(connection_params=None, prefetch_count=None,
                retry_delay=0.01, connection_attempts=3)

        self.broker._connect = mock.Mock()
        self.connect_deferred = defer.Deferred()
        self.broker._connect.return_value = self.connect_deferred

        self.confirm_deferreds = []
        def make_deferred(*args):
            deferred = defer.Deferred()
            self.confirm_deferreds.append(deferred)
            return deferred
        self.broker.raw_publish = make_deferred

        self.message = mock.Mock()

    def test_publisher_confirms(self):
        publish_deferred = self.broker.publish(exchange_name=None,
                routing_key=None, message=self.message)
        self.assertFalse(publish_deferred.called)

        self.connect_deferred.callback(None)
        self.assertFalse(publish_deferred.called)
        self.assertEqual(len(self.confirm_deferreds), 1)

        self.confirm_deferreds[0].callback(None)
        self.assertTrue(publish_deferred.called)

    def test_publisher_denial(self):
        publish_deferred = self.broker.publish(exchange_name=None,
                routing_key=None, message=self.message)
        self.connect_deferred.callback(None)

        self.assertFalse(publish_deferred.called)
        self.assertEqual(len(self.confirm_deferreds), 1)

        self.confirm_deferreds[0].errback(RuntimeError)
        self.assertTrue(publish_deferred.called)

        self.assertTrue(isinstance(publish_deferred.result, Failure))

        # add Errbacks so twisted doesn't get the exceptions
        self.confirm_deferreds[0].addErrback(lambda _: None)
        publish_deferred.addErrback(lambda _: None)

    def test_multiple_confirms(self):
        publish_deferreds = []
        publish_deferreds.append(self.broker.publish(exchange_name=None,
            routing_key=None, message=self.message))
        publish_deferreds.append(self.broker.publish(exchange_name=None,
            routing_key=None, message=self.message))
        self.connect_deferred.callback(None)

        self.assertFalse(publish_deferreds[0].called)
        self.assertFalse(publish_deferreds[1].called)
        self.assertEqual(len(self.confirm_deferreds), 2)

        self.confirm_deferreds[0].callback(None)
        self.assertTrue(publish_deferreds[0].called)
        self.assertFalse(publish_deferreds[1].called)

        self.confirm_deferreds[1].callback(None)
        self.assertTrue(publish_deferreds[0].called)
        self.assertTrue(publish_deferreds[1].called)

    def test_multiple_confirms_single_connect(self):
        publish_deferreds = []
        publish_deferreds.append(self.broker.publish(exchange_name=None,
            routing_key=None, message=self.message))
        self.connect_deferred.callback(None)

        self.confirm_deferreds[0].callback(None)
        self.assertTrue(publish_deferreds[0].called)
        self.assertEqual(len(self.confirm_deferreds), 1)

        publish_deferreds.append(self.broker.publish(exchange_name=None,
            routing_key=None, message=self.message))
        self.assertEqual(len(self.confirm_deferreds), 2)

        self.confirm_deferreds[1].callback(None)
        self.assertTrue(publish_deferreds[0].called)
        self.assertTrue(publish_deferreds[1].called)


class MoreAmqpBrokerTest(unittest.TestCase):
    def setUp(self):
        self.broker = AmqpBroker(connection_params=mock.Mock(), prefetch_count=None,
                retry_delay=0.01, connection_attempts=3)
        self.broker._get_connection_params = mock.Mock()

    def test_reconnect(self):
        self._connect_deferreds = []
        def make_deferred(*args):
            deferred = defer.Deferred()
            self._connect_deferreds.append(deferred)
            return deferred

        def fake_client_creator(*args, **kwargs):
            m = mock.Mock()
            m.connectTCP = make_deferred
            return m

        self.assertFalse(amqp_broker.protocol.ClientCreator is fake_client_creator)
        with mock.patch('twisted.internet.protocol.ClientCreator',
                new=fake_client_creator):
            self.assertTrue(amqp_broker.protocol.ClientCreator is fake_client_creator)
            connect_deferred = self.broker.connect()
            self.assertEqual(len(self._connect_deferreds), 1)

            # only one connection attempt at a time.
            same_connect_deferred = self.broker.connect()
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
                self._connect_deferreds[2].errback(RuntimeError('bad'))
                self.assertEqual(len(self._connect_deferreds), 3)
                self.assertFalse(connect_deferred.called)