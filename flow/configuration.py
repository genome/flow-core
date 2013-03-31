import argparse
import os
import pkg_resources
import statsd
import sys
import yaml

import flow.commands.base

import logging.config

LOG = logging.getLogger(__name__)

_DEFAULT_CONFIG_FILE = '/etc/flow.yaml'

def load_config(configuration_filename):
    if configuration_filename is None:
        configuration_filename = os.getenv('FLOW_CONFIG')
    if configuration_filename is None:
        configuration_filename = _DEFAULT_CONFIG_FILE

    try:
        with open(configuration_filename) as f:
            configuration_dict = yaml.load(f)
    except IOError:
        raise ValueError("No configuration file provided "
                "(searched FLOW_CONFIG and '%s' for default)"
                % _DEFAULT_CONFIG_FILE)

    return configuration_dict


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


def initialize_logging(config, logging_mode):
    logging_config = config['logging_configurations'][logging_mode]
    logging.config.dictConfig(logging_config)


def initialize_statsd(config):
    try:
        statsd_settings = config['statsd_configuration']
    except KeyError:
        statsd_settings = {}
        LOG.warn('No statsd settings found in configuration.')
    statsd.init_statsd(statsd_settings)
