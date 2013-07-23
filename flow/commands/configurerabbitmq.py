from flow.commands.base import CommandBase
from flow.configuration.inject.broker import BrokerConfiguration
from flow.configuration.settings.injector import setting
from injector import inject
from twisted.internet import defer

import flow.interfaces
import logging
import time


LOG = logging.getLogger(__name__)


@inject(broker=flow.interfaces.IBroker,
        binding_config=setting('bindings'))
class ConfigureRabbitMQCommand(CommandBase):
    injector_modules = [
        BrokerConfiguration,
    ]


    @staticmethod
    def annotate_parser(parser):
        pass


    @defer.inlineCallbacks
    def _execute(self, parsed_arguments):
        exchanges, queues, bindings = self._parse_config()

        yield self._declare_exchanges(exchanges)
        yield self._declare_queues(queues)
        yield self._declare_bindings(bindings)

        # XXX Dirty workaround.  I can't tell why the bindings aren't declared
        # by the time we get here.
        time.sleep(2)


    def _parse_config(self):
        exchanges = set()
        queues = set()
        bindings = set()

        for exchange_name, queue_bindings in self.binding_config.iteritems():
            exchanges.add(exchange_name)

            for queue_name, topics in queue_bindings.iteritems():
                queues.add(queue_name)

                for topic in topics:
                    bindings.add( (queue_name, exchange_name, topic) )

        return exchanges, queues, bindings

    def _declare_exchanges(self, exchanges):
        deferreds = []
        deferreds.append(self.broker.channel.declare_exchange('alt'))

        arguments = {'alternate-exchange': 'alt'}
        deferreds.append(self.broker.channel.declare_exchange(
            'dead', arguments=arguments))

        for exchange_name in exchanges:
            deferreds.append(self.broker.channel.declare_exchange(
                exchange_name, arguments=arguments))

        return defer.DeferredList(deferreds)

    def _declare_queues(self, queues):
        deferreds = []
        deferreds.append(self.broker.channel.declare_queue(
            'missing_routing_key'))

        arguments = {'x-dead-letter-exchange': 'dead'}
        for queue in queues:
            deferreds.append(self.broker.channel.declare_queue(
                queue, arguments=arguments))
            deferreds.append(self.broker.channel.declare_queue('dead_' + queue))

        return defer.DeferredList(deferreds)

    def _declare_bindings(self, bindings):
        deferreds = []
        deferreds.append(self.broker.channel.bind_queue(
            'missing_routing_key', 'alt', '#'))

        for queue, exchange, topic in bindings:
            deferreds.append(self.broker.channel.bind_queue(queue, exchange,
                topic))
            deferreds.append(self.broker.channel.bind_queue(
                'dead_' + queue, 'dead', topic))

        return defer.DeferredList(deferreds)
