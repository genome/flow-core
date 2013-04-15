from flow.command_runner.handler import CommandLineSubmitMessageHandler
from flow.commands.service import ServiceCommand
from flow.conf.broker import BrokerConfiguration
from flow.conf.fork_executor import ForkExecutorConfiguration
from flow.conf.redis_conf import RedisConfiguration
from flow.interfaces import IOrchestrator, ILocalShellCommand, IGridShellCommand
from injector import inject

import logging

LOG = logging.getLogger(__name__)


@inject(orchestrator=IOrchestrator)
class ForkShellCommand(ServiceCommand):
    injector_modules = [
            BrokerConfiguration,
            RedisConfiguration,
            ForkExecutorConfiguration,
    ]

    def __call__(self, *args, **kwargs):
        self.service_interfaces = {}

        self.handlers = [self.injector.get(CommandLineSubmitMessageHandler)]

        return ServiceCommand.__call__(self, *args, **kwargs)
