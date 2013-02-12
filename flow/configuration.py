import argparse
import os
import pkg_resources
import sys
import yaml

import flow.commands.base

from logging.config import dictConfig


def load_config(configuration_filename):
    if configuration_filename is None:
        configuration_filename = os.getenv('FLOW_CONFIG')
    if configuration_filename is None:
        raise ValueError("No configuration filename provided, and FLOW_CONFIG environment variable not set")

    with open(configuration_filename) as f:
        configuration_dict = yaml.load(f)

    return configuration_dict


def setup_logging(logging_dict):
    dictConfig(logging_dict)


def load_commands(command_category='flow.commands'):
    commands = {}
    for ep in pkg_resources.iter_entry_points(command_category):
        try:
            command = ep.load()
            assert issubclass(command, flow.commands.base.CommandBase)
        except:
            sys.stderr.write('Failed to load command: %s\n' % ep.name)
            raise
        commands[ep.name] = command
    return commands

def create_parser(commands):
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default=None, help='Configuration file')
    parser.add_argument('--logging-mode',
            help='Logging configuration to use')

    subparsers = parser.add_subparsers(title='Subcommands', dest='command_name')
    for name, command in commands.iteritems():
        command_parser = subparsers.add_parser(name)
        # TODO (depends on python 2.7) add aliases=command.aliases
        command_parser.set_defaults(command=command)
        command.annotate_parser(command_parser)

    return parser
