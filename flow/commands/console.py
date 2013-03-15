from IPython import embed

import flow.redisom as rom

from flow.commands.base import CommandBase
import logging

LOG = logging.getLogger(__name__)


class ConsoleCommand(CommandBase):
    def __init__(self, storage=None, broker=None):
        self.storage = storage
        self.broker = broker

    @staticmethod
    def annotate_parser(parser):
        parser.add_argument('--net-key', '-n', default=None,
                help='Load the net associated with this key '
                     'into the "net" variable.')

        parser.add_argument('--object', '-o', default=None, nargs=2,
                metavar=('NAME', 'KEY'),
                help='Load the object associated with KEY '
                     'into the NAME variable.')

    def __call__(self, parsed_arguments):
        namespace = {
                'storage': self.storage,
                'broker': self.broker,
                'rom': rom,
                }

        if parsed_arguments.net_key:
            namespace['net'] = self.get_key(parsed_arguments.net_key)

        if parsed_arguments.object:
            namespace[parsed_arguments.object[0]] = self.get_key(
                    parsed_arguments.object[1])

        embed(user_ns=namespace, display_banner=False)


    def get_key(self, key):
        try:
            return rom.get_object(self.storage, key)
        except KeyError:
            LOG.error('Key (%s) not found.', key)
        return None
