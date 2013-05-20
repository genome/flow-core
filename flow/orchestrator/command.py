from flow.commands.service import ServiceCommand
from flow.configuration.inject.broker import BrokerConfiguration
from flow.configuration.inject.redis_conf import RedisConfiguration
from flow.configuration.inject.service_locator import ServiceLocatorConfiguration
from flow.orchestrator.handlers import PetriCreateTokenHandler
from flow.orchestrator.handlers import PetriNotifyPlaceHandler
from flow.orchestrator.handlers import PetriNotifyTransitionHandler

import logging


LOG = logging.getLogger(__name__)


class OrchestratorCommand(ServiceCommand):
    injector_modules = [
            BrokerConfiguration,
            RedisConfiguration,
            ServiceLocatorConfiguration,
    ]

    def _setup(self, *args, **kwargs):
        self.handlers = [
                self.injector.get(PetriCreateTokenHandler),
                self.injector.get(PetriNotifyPlaceHandler),
                self.injector.get(PetriNotifyTransitionHandler)
        ]

        return ServiceCommand._setup(self, *args, **kwargs)
