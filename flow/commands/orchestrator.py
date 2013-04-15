from flow.commands.service import ServiceCommand
from flow.configuration.inject.broker import BrokerConfiguration
from flow.configuration.inject.redis_conf import RedisConfiguration
from flow.configuration.inject.shell import LocalShellConfiguration, LSFConfiguration
from flow.interfaces import IOrchestrator, ILocalShellCommand, IGridShellCommand
from injector import inject

import logging

LOG = logging.getLogger(__name__)


@inject(orchestrator=IOrchestrator,
        fork=ILocalShellCommand,
        grid=IGridShellCommand,
#        historian=IWorkflowHistorian,
        )
class OrchestratorCommand(ServiceCommand):
    injector_modules = [
            BrokerConfiguration,
            RedisConfiguration,
            LocalShellConfiguration,
            LSFConfiguration,
    ]

    def __call__(self, *args, **kwargs):
        self.service_interfaces = {
                'orchestrator': self.orchestrator,
                'fork': self.fork,
                'lsf': self.grid,
#                'workflow_historian': self.historian,
        }

        self.handlers = []

        return ServiceCommand.__call__(self, *args, **kwargs)
