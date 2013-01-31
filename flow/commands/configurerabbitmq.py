import argparse
from flow.commands.base import CommandBase
import logging
import time
import uuid

LOG = logging.getLogger()


class ConfigureRabbitMQCommand(CommandBase):
    def __init__(self, bindings={}, vhost=''):
        print "in __init__ vhost %s bindings %s" % (vhost, bindings)

    @staticmethod
    def annotate_parser(parser):
        parser.add_argument('--output_filename', '-o',
                            default=None, help='Filename to write to. Defaults to STDOUT')

    def __call__(self, parsed_arguments):
        print "in __call__ self %s parsed_args %s" % (self, parsed_arguments)

        return 0
