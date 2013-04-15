from flow.command_runner.handler import CommandLineSubmitMessageHandler
from flow.commands.service import ServiceCommand
from flow.configuration.inject.broker import BrokerConfiguration
from flow.configuration.inject.fork_executor import ForkExecutorConfiguration
from flow.configuration.inject.redis_conf import RedisConfiguration
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

        self.handlers = [self.inject.get(CommandLineSubmitMessageHandler)]

        return ServiceCommand.__call__(self, *args, **kwargs)
