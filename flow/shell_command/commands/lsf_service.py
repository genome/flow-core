from flow.commands.service import ServiceCommand
from flow.configuration.inject.broker import BrokerConfiguration
from flow.configuration.inject.redis_conf import RedisConfiguration
from flow.configuration.inject.service_locator import ServiceLocatorConfiguration
from flow.shell_command.lsf.configuration import LSFExecutorConfiguration
from flow.shell_command.handler import LSFShellCommandMessageHandler

import logging


LOG = logging.getLogger(__name__)


class LSFShellCommand(ServiceCommand):
    injector_modules = [
            BrokerConfiguration,
            RedisConfiguration,
            LSFExecutorConfiguration,
            ServiceLocatorConfiguration,
    ]

    def _setup(self, *args, **kwargs):
        self.handlers = [self.injector.get(LSFShellCommandMessageHandler)]

        return ServiceCommand._setup(self, *args, **kwargs)
