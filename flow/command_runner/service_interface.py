from flow.command_runner.messages import CommandLineSubmitMessage
from injector import inject, Setting

import flow.command_runner.executors.nets
import flow.interfaces
import logging


LOG = logging.getLogger(__name__)


@inject(broker=flow.interfaces.IBroker)
class CommandLineServiceInterface(object):
    def submit(self, command_line, net_key=None, response_places=None,
            **executor_options):
        message = CommandLineSubmitMessage(
                command_line=command_line,
                net_key=net_key,
                response_places=response_places,
                executor_options=executor_options)

        self.broker.publish(self.exchange, self.submit_routing_key, message)


@inject(exchange=Setting('shell.fork.exchange'),
        submit_routing_key=Setting('shell.fork.submit_routing_key'))
class ForkCommandLineServiceInterface(CommandLineServiceInterface):
    pass


@inject(exchange=Setting('shell.lsf.exchange'),
        submit_routing_key=Setting('shell.lsf.submit_routing_key'))
class LSFCommandLineServiceInterface(CommandLineServiceInterface):
    pass
