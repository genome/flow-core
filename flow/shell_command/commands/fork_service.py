from flow.shell_command.handler import ForkShellCommandMessageHandler
from flow.commands.service import ServiceCommand
from flow.configuration.inject.broker import BrokerConfiguration
from flow.configuration.inject.fork_executor import ForkExecutorConfiguration
from flow.configuration.inject.redis_conf import RedisConfiguration
from flow.configuration.inject.service_locator import ServiceLocatorConfiguration
from injector import inject

import logging

LOG = logging.getLogger(__name__)


class ForkShellCommand(ServiceCommand):
    injector_modules = [
            BrokerConfiguration,
            RedisConfiguration,
            ForkExecutorConfiguration,
            ServiceLocatorConfiguration,
    ]

    def __call__(self, *args, **kwargs):
        self.handlers = [self.injector.get(ForkShellCommandMessageHandler)]

        return ServiceCommand.__call__(self, *args, **kwargs)
