from flow.configuration.settings.injector import setting
from flow.shell_command.messages import ShellCommandSubmitMessage
from injector import inject

import flow.shell_command.executors.nets
import flow.interfaces
import logging


LOG = logging.getLogger(__name__)


@inject(broker=flow.interfaces.IBroker)
class ShellCommandServiceInterface(object):
    def submit(self, command_line, net_key=None, response_places=None,
            **executor_options):
        message = ShellCommandSubmitMessage(
                command_line=command_line,
                net_key=net_key,
                response_places=response_places,
                executor_options=executor_options)

        self.broker.publish(self.exchange, self.submit_routing_key, message)


@inject(exchange=setting('shell_command.fork.exchange'),
        submit_routing_key=setting('shell_command.fork.submit_routing_key'))
class ForkShellCommandServiceInterface(ShellCommandServiceInterface):
    pass


@inject(exchange=setting('shell_command.lsf.exchange'),
        submit_routing_key=setting('shell_command.lsf.submit_routing_key'))
class LSFShellCommandServiceInterface(ShellCommandServiceInterface):
    pass
