from flow.configuration.settings.injector import setting
from flow.shell_command.interfaces import IShellCommand
from flow.shell_command.messages import ShellCommandSubmitMessage
from injector import inject

import flow.interfaces
import logging


LOG = logging.getLogger(__name__)


@inject(broker=flow.interfaces.IBroker)
class ShellCommandServiceInterface(IShellCommand):
    def submit(self, **kwargs):
        message = ShellCommandSubmitMessage(**kwargs)

        return self.broker.publish(self.exchange,
                self.submit_routing_key, message)


@inject(exchange=setting('shell_command.fork.exchange'),
        submit_routing_key=setting('shell_command.fork.submit_routing_key'))
class ForkShellCommandServiceInterface(ShellCommandServiceInterface):
    pass


@inject(exchange=setting('shell_command.lsf.exchange'),
        submit_routing_key=setting('shell_command.lsf.submit_routing_key'))
class LSFShellCommandServiceInterface(ShellCommandServiceInterface):
    def submit(self, **kwargs):
        # XXX Additional message validation:
        #       executor_data must contain response places, net key, etc.
        #       executor_data must not contain working_directory (easy mistake)

        return ShellCommandServiceInterface.submit(self, **kwargs)
