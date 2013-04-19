from flow.commands.service import ServiceCommand
from flow.configuration.inject.broker import BrokerConfiguration
from flow.configuration.inject.redis_conf import RedisConfiguration
from flow.configuration.inject.service_locator import ServiceLocatorConfiguration
from flow.orchestrator.handlers import PetriSetTokenHandler, PetriNotifyTransitionHandler
from injector import inject, Injector

import flow.interfaces
import logging


LOG = logging.getLogger(__name__)


#@inject(injector=Injector)
class OrchestratorCommand(ServiceCommand):
    injector_modules = [
            BrokerConfiguration,
            RedisConfiguration,
            ServiceLocatorConfiguration,
    ]

    def __call__(self, *args, **kwargs):
        self.handlers = [
                self.injector.get(PetriSetTokenHandler),
                self.injector.get(PetriNotifyTransitionHandler)
        ]

        return ServiceCommand.__call__(self, *args, **kwargs)
