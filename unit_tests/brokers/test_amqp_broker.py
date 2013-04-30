import unittest
try:
    from unittest import mock
except:
    import mock

from twisted.python.failure import Failure
from twisted.internet import defer

from flow.brokers.amqp_broker import AmqpBroker

class AmqpBrokerTest(unittest.TestCase):
    def setUp(self):
        self.broker = AmqpBroker(connection_params=None, prefetch_count=None)

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
