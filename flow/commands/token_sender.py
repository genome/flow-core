from flow.commands.base import CommandBase
from flow.configuration.inject.broker import BrokerConfiguration
from flow.configuration.settings.injector import setting
from flow.service_locator import ServiceLocator
from injector import inject

import logging


LOG = logging.getLogger(__name__)


@inject(service_locator=ServiceLocator,
        exchange=setting('send_token.exchange'),
        routing_key=setting('send_token.routing_key'))
class TokenSenderCommand(CommandBase):
    injector_modules = [
            BrokerConfiguration,
    ]

    def send_token(self, net_key=None, place_idx=None, data=None, color=None):
        orchestrator = self.service_locator['orchestrator']
        LOG.info("Sending command response token to net %s, place %r",
                net_key, place_idx)

        # XXX This interface has changed
        return orchestrator.create_token(net_key=net_key,
                place_idx=int(place_idx), data=data,
                data_type="output", token_color=color)
