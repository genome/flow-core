from flow.commands.base import CommandBase
from flow.configuration.inject.broker import BrokerConfiguration
from flow.configuration.inject.redis_conf import RedisConfiguration
from flow.configuration.settings.injector import setting
from flow.service_locator import ServiceLocator
from flow.petri.netbase import Token, SetTokenMessage
from injector import inject

import flow.interfaces
import logging


LOG = logging.getLogger(__name__)


@inject(service_locator=ServiceLocator,
        storage=flow.interfaces.IStorage,
        exchange=setting('send_token.exchange'),
        routing_key=setting('send_token.routing_key'))
class TokenSenderCommand(CommandBase):
    injector_modules = [
            BrokerConfiguration,
            RedisConfiguration,
    ]

    def send_token(self, net_key=None, place_idx=None, data=None, color=None):
        orchestrator = self.service_locator['orchestrator']

        token = Token.create(self.storage, data=data, data_type="output", color_idx=color)

        LOG.info("Sending command response token %s to net %s, place %r",
                token.key, net_key, place_idx)
        return orchestrator.set_token(net_key=net_key, place_idx=int(place_idx),
                token_key=token.key)
