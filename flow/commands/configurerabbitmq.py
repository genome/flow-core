from flow.commands.base import CommandBase
from flow.configuration.inject.broker import BrokerConfiguration
from flow.configuration.settings.injector import setting
from flow.util.exit import exit_process
from flow import exit_codes
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


    def _execute(self, parsed_arguments):
        exchanges, queues, bindings = self._parse_config()

        LOG.debug("Parsed config")

        deferreds = []
        deferreds.append(self._declare_exchanges(exchanges))
        deferreds.append(self._declare_queues(queues))
        dlist = defer.DeferredList(deferreds)

        _execute_deferred = defer.Deferred()
        dlist.addCallback(self._declare_bindings, bindings=bindings,
                    _execute_deferred=_execute_deferred)
        dlist.addErrback(self._exit)

        return _execute_deferred

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
        LOG.debug("Declaring Exchange: alt")
        deferreds.append(self.broker.channel.declare_exchange('alt'))

        arguments = {'alternate-exchange': 'alt'}
        LOG.debug("Declaring Exchange: dead")
        deferreds.append(self.broker.channel.declare_exchange(
            'dead', arguments=arguments))

        for exchange_name in exchanges:
            LOG.debug("Declaring Exchange: %s", exchange_name)
            deferreds.append(self.broker.channel.declare_exchange(
                exchange_name, arguments=arguments))

        return defer.DeferredList(deferreds)

    def _declare_queues(self, queues):
        deferreds = []
        deferreds.append(self.broker.channel.declare_queue(
            'missing_routing_key'))

        arguments = {'x-dead-letter-exchange': 'dead'}
        for queue in queues:
            LOG.debug("Declaring queue %s", queue)
            deferreds.append(self.broker.channel.declare_queue(
                queue, arguments=arguments))
            deferreds.append(self.broker.channel.declare_queue('dead_' + queue))

        return defer.DeferredList(deferreds)

    def _declare_bindings(self, _callback, bindings, _execute_deferred):
        deferreds = []
        deferreds.append(self.broker.channel.bind_queue(
            'missing_routing_key', 'alt', '#'))

        for queue, exchange, topic in bindings:
            deferreds.append(self.broker.channel.bind_queue(queue, exchange,
                topic))
            deferreds.append(self.broker.channel.bind_queue(
                'dead_' + queue, 'dead', topic))

        dlist = defer.DeferredList(deferreds)
        dlist.addCallback(self._wait_and_fire_deferred,
               _execute_deferred=_execute_deferred)
        dlist.addErrback(self._exit)

        return _callback

    def _wait_and_fire_deferred(self, _callback, _execute_deferred):
        # XXX Dirty workaround.  I can't tell why the bindings aren't declared
        # by the time we get here.
        time.sleep(2)
        _execute_deferred.callback(None)

    def _exit(self, error):
        LOG.critical("Unexepected error in configurerabbitmq.\n%s", error.getTraceback())
        exit_process(exit_codes.EXECUTE_FAILURE)
