from flow.commands.configurerabbitmq import ConfigureRabbitMQCommand
from twisted.internet import defer

import mock
import unittest


class ConfigureRabbitMQCommandTest(unittest.TestCase):
    def setUp(self):
        self.broker = mock.MagicMock()
        self.broker.connect.return_value = defer.succeed(None)

        self.bindings = {
            'X-NAMES': {
                'Q-TIP': ['routing.key'],
                'H-TOP': ['another.key.routed'],
            },
        }

        self.command = ConfigureRabbitMQCommand(
                broker=self.broker, binding_config=self.bindings)

    def test_execute(self):
        declare_exchanges = mock.Mock()
        declare_exchanges.return_value = defer.succeed(None)

        declare_queues = mock.Mock()
        declare_queues.return_value = defer.succeed(None)

        declare_bindings = mock.Mock()
        declare_bindings.return_value = defer.succeed(None)

        self.command._declare_exchanges = declare_exchanges
        self.command._declare_queues = declare_queues
        self.command._declare_bindings = declare_bindings
        self.command._execute(None)

        declare_exchanges.assert_called_once()
        declare_queues.assert_called_once()
        declare_bindings.assert_called_once()

    def test_parse_config(self):
        expected_exchanges = {'X-NAMES'}
        expected_queues = {'Q-TIP', 'H-TOP'}
        expected_bindings = {
            ('Q-TIP', 'X-NAMES', 'routing.key'),
            ('H-TOP', 'X-NAMES', 'another.key.routed'),
        }

        exchanges, queues, bindings = self.command._parse_config()

        self.assertItemsEqual(expected_exchanges, exchanges)
        self.assertItemsEqual(expected_queues, queues)
        self.assertItemsEqual(expected_bindings, bindings)

    def test_declare_exchanges(self):
        exchanges = {'X-NAMES'}
        self.command._declare_exchanges(exchanges)

        arguments = {'alternate-exchange': 'alt'}

        self.broker.declare_exchange.assert_any_call('alt')
        self.broker.declare_exchange.assert_any_call(
                'dead', arguments=arguments)
        self.broker.declare_exchange.assert_any_call(
                'X-NAMES', arguments=arguments)

    def test_declare_queues(self):
        queues = {'Q-TIP', 'H-TOP'}

        self.command._declare_queues(queues)

        self.broker.declare_queue.assert_any_call('missing_routing_key')

        arguments = {'x-dead-letter-exchange': 'dead'}
        for q in queues:
            self.broker.declare_queue.assert_any_call(q, arguments=arguments)
            self.broker.declare_queue.assert_any_call('dead_%s' % q)


    def test_declare_bindings(self):
        bindings = {
            ('Q-TIP', 'X-NAMES', 'routing.key'),
            ('H-TOP', 'X-NAMES', 'another.key.routed'),
        }

        self.command._declare_bindings(bindings)

        self.broker.bind_queue.assert_any_call(
                'missing_routing_key', 'alt', '#')
        for q, x, k in bindings:
            self.broker.bind_queue.assert_any_call(q, x, k)
            self.broker.bind_queue.assert_any_call('dead_%s' % q, 'dead', k)


if '__main__' == __name__:
    unittest.main()
