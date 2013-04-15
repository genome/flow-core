from flow.commands.base import CommandBase
from flow.conf.redis_conf import RedisConfiguration
from injector import inject, Setting

import flow.interfaces
import flow.redisom as rom
import logging
import sys


LOG = logging.getLogger(__name__)


@inject(storage=flow.interfaces.IStorage)
class GraphCommand(CommandBase):
    injector_modules = [
            RedisConfiguration,
    ]

    @staticmethod
    def annotate_parser(parser):
        parser.add_argument('netkey',
                help='Load the net associated with this key '
                     'into the "net" variable.')
        parser.add_argument('--output-filename', '-o', default=None,
                help='Name of the output file (defaults to STDOUT)')
        parser.add_argument('--format', '-f', default='ps',
                help='Output format for graph')

    def __call__(self, parsed_arguments):
        net = rom.get_object(self.storage, parsed_arguments.netkey)

        s = net.graph().draw(format=parsed_arguments.format, prog='dot')
        if parsed_arguments.output_filename:
            f = open(parsed_arguments.output_filename, 'w')
        else:
            f = sys.stdout
        f.write(s)
