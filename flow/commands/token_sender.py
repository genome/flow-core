import logging

from flow.commands.base import CommandBase
from flow.petri.safenet import Token, SetTokenMessage

LOG = logging.getLogger(__name__)

class TokenSenderCommand(CommandBase):
    def __init__(self, broker=None, storage=None, routing_key=None, exchange=None):
        self.broker = broker
        self.storage = storage
        self.routing_key = routing_key
        self.exchange = exchange

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
