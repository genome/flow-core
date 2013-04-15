from flow.commands.token_sender import TokenSenderCommand
from flow.petri import Token

import logging


LOG = logging.getLogger(__name__)


class SetTokenCommand(TokenSenderCommand):
    @staticmethod
    def annotate_parser(parser):
        parser.add_argument('--net-key', '-n', required=True,
                help='The relevant net key')
        parser.add_argument('--place-idx', '-p', type=int, required=True,
                help='The place index within the net to send the token to')
        parser.add_argument('--token-key', '-t', default=None,
                help='Optional existing token key (by default, a new token is '
                'created)')

    def __call__(self, parsed_arguments):
        if parsed_arguments.token_key:
            token_key = parsed_arguments.token_key
        else:
            token_key = str(Token.create(self.storage).key)

        self.send_token(parsed_arguments.net_key, parsed_arguments.place_idx,
                token_key)

        return 0
