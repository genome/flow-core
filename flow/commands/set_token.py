from flow.commands.base import CommandBase
from flow.petri import Token, SetTokenMessage
import logging

LOG = logging.getLogger(__name__)


class SetTokenCommand(CommandBase):
    def __init__(self, broker=None, storage=None, routing_key=None):

        self.broker = broker
        self.storage = storage
        self.routing_key = routing_key

    @staticmethod
    def annotate_parser(parser):
        parser.add_argument('--net-key', '-n',
                help='The relevant net key')
        parser.add_argument('--place-idx', '-p', type=int,
                help='The place index within the net to send the token to')
        parser.add_argument('--token-key', '-t', default=None,
                help='Optional existing token key (by default, a new token is '
                'created)')

    def __call__(self, parsed_arguments):
        if not parsed_arguments.token_key:
            parsed_arguments.token_key = str(Token.create(self.storage).key)

        self.broker.connect()

        message = SetTokenMessage(net_key=parsed_arguments.net_key,
                place_idx=parsed_arguments.place_idx,
                token_key=parsed_arguments.token_key)
        self.broker.publish(self.routing_key, message)

        self.broker.disconnect()

        return 0
