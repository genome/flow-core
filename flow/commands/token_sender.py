from flow.commands.base import CommandBase
from flow.conf.broker import BlockingBrokerConfiguration
from flow.conf.redis_conf import RedisConfiguration
from flow.petri.netbase import Token, SetTokenMessage
from injector import inject, Setting

import flow.interfaces
import logging


LOG = logging.getLogger(__name__)


@inject(broker=flow.interfaces.IBroker, storage=flow.interfaces.IStorage,
        exchange=Setting('send_token.exchange'),
        routing_key=Setting('send_token.routing_key'))
class TokenSenderCommand(CommandBase):
    injector_modules = [
            BlockingBrokerConfiguration,
            RedisConfiguration,
    ]

    def send_token(self, net_key=None, place_idx=None, data=None):
        self.broker.connect()
        token = Token.create(self.storage, data=data, data_type="output")

        LOG.info("Sending command response token %s to net %s, place %r",
                token.key, net_key, place_idx)
        message = SetTokenMessage(net_key=net_key, place_idx=place_idx,
                token_key=token.key)
        self.broker.publish(exchange_name=self.exchange, routing_key=self.routing_key,
                message=message)
        self.broker.disconnect()
