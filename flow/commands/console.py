from IPython import embed
from flow.commands.base import CommandBase
from flow.configuration.inject.broker import BlockingBrokerConfiguration
from flow.configuration.inject.redis_conf import RedisConfiguration
from injector import inject, Injector

import flow.interfaces
import flow.redisom
import logging

LOG = logging.getLogger(__name__)


@inject(storage=flow.interfaces.IStorage, broker=flow.interfaces.IBroker,
        injector=Injector)
class ConsoleCommand(CommandBase):
    injector_modules = [
            BlockingBrokerConfiguration,
            RedisConfiguration,
    ]

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
                'broker': self.broker,
                'get_object': self.get_key,
                'injector': self.inject,
                'interfaces': flow.interfaces,
                'rom': flow.redisom,
                'storage': self.storage,
                }

        if parsed_arguments.net_key:
            namespace['net'] = self.get_key(parsed_arguments.net_key)

        if parsed_arguments.object:
            namespace[parsed_arguments.object[0]] = self.get_key(
                    parsed_arguments.object[1])

        embed(user_ns=namespace, display_banner=False)


    def get_key(self, key):
        try:
            return flow.redisom.get_object(self.storage, key)
        except KeyError:
            LOG.error('Key (%s) not found.', key)
        return None
