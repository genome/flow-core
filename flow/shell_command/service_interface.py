from flow.configuration.settings.injector import setting
from flow.shell_command.interfaces import IShellCommand
from flow.shell_command.messages import ShellCommandSubmitMessage
from injector import inject

import flow.interfaces
import logging
import os


LOG = logging.getLogger(__name__)


@inject(broker=flow.interfaces.IBroker)
class ShellCommandServiceInterface(IShellCommand):
    def submit(self, **kwargs):
        message = ShellCommandSubmitMessage(**kwargs)

        return self.broker.publish(self.exchange, self.submit_routing_key, message)


@inject(exchange=setting('shell_command.fork.exchange'),
        submit_routing_key=setting('shell_command.fork.submit_routing_key'))
class ForkShellCommandServiceInterface(ShellCommandServiceInterface):
    pass


@inject(exchange=setting('shell_command.lsf.exchange'),
        submit_routing_key=setting('shell_command.lsf.submit_routing_key'))
class LSFShellCommandServiceInterface(ShellCommandServiceInterface):
    pass
