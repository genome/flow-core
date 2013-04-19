from flow.shell_command.handler import LSFShellCommandMessageHandler
from flow.commands.service import ServiceCommand
from flow.configuration.inject.broker import BrokerConfiguration
from flow.configuration.inject.lsf_executor import LSFExecutorConfiguration
from flow.configuration.inject.redis_conf import RedisConfiguration
from flow.configuration.inject.service_locator import ServiceLocatorConfiguration
from injector import inject

import logging

LOG = logging.getLogger(__name__)


class LSFShellCommand(ServiceCommand):
    injector_modules = [
            BrokerConfiguration,
            RedisConfiguration,
            LSFExecutorConfiguration,
            ServiceLocatorConfiguration,
    ]

    def __call__(self, *args, **kwargs):
        self.handlers = [self.injector.get(LSFShellCommandMessageHandler)]

        return ServiceCommand.__call__(self, *args, **kwargs)
