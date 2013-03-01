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

    def __call__(self, parsed_arguments):
        namespace = {
                'storage': self.storage,
                'broker': self.broker
                }

        if parsed_arguments.net_key:
            try:
                namespace['net'] = rom.get_object(
                        self.storage, parsed_arguments.net_key)
            except KeyError:
                LOG.error('Net key (%s) not found.', parsed_arguments.net_key)

        embed(user_ns=namespace, display_banner=False)
