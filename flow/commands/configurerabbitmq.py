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
        self._parse_config()

        yield self.broker.connect()

        yield self._declare_exchanges()
        yield self._declare_queues()
        yield self._declare_bindings()

        # XXX Dirty workaround.  I can't tell why the bindings aren't declared
        # by the time we get here.
        time.sleep(2)


    def _parse_config(self):
        self.exchanges = set()
        self.queues = set()
        self.bindings = set()

        for exchange_name, queue_bindings in self.binding_config.iteritems():
            self.exchanges.add(exchange_name)

            for queue_name, topics in queue_bindings.iteritems():
                self.queues.add(queue_name)

                for topic in topics:
                    self.bindings.add( (queue_name, exchange_name, topic) )

    def _declare_exchanges(self):
        deferreds = []
        deferreds.append(self.broker.declare_exchange('alt'))

        arguments = {'alternate-exchange': 'alt'}
        deferreds.append(self.broker.declare_exchange(
            'dead', arguments=arguments))

        for exchange_name in self.exchanges:
            deferreds.append(self.broker.declare_exchange(
                exchange_name, arguments=arguments))

        return defer.DeferredList(deferreds)

    def _declare_queues(self):
        deferreds = []
        deferreds.append(self.broker.declare_queue('missing_routing_key'))

        arguments = {'x-dead-letter-exchange': 'dead'}
        for queue in self.queues:
            deferreds.append(self.broker.declare_queue(
                queue, arguments=arguments))
            deferreds.append(self.broker.declare_queue('dead_' + queue))

        return defer.DeferredList(deferreds)

    def _declare_bindings(self):
        deferreds = []
        deferreds.append(self.broker.bind_queue(
            'missing_routing_key', 'alt', '#'))

        for queue, exchange, topic in self.bindings:
            deferreds.append(self.broker.bind_queue(queue, exchange, topic))
            deferreds.append(self.broker.bind_queue(
                'dead_' + queue, 'dead', topic))

        return defer.DeferredList(deferreds)
