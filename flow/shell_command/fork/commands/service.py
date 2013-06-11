from flow.commands.service import ServiceCommand
from flow.configuration.inject.broker import BrokerConfiguration
from flow.configuration.inject.redis_conf import RedisConfiguration
from flow.configuration.inject.service_locator import ServiceLocatorConfiguration
from flow.shell_command.fork.configuration import ForkExecutorConfiguration
from flow.shell_command.handler import ForkShellCommandMessageHandler

import logging


LOG = logging.getLogger(__name__)


class ForkShellCommand(ServiceCommand):
    injector_modules = [
            BrokerConfiguration,
            RedisConfiguration,
            ForkExecutorConfiguration,
            ServiceLocatorConfiguration,
    ]

    def _setup(self, *args, **kwargs):
        self.handlers = [self.injector.get(ForkShellCommandMessageHandler)]

        return ServiceCommand._setup(self, *args, **kwargs)
