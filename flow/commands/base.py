from abc import ABCMeta, abstractmethod
import flow.configuration
from flow.factories import dictionary_factory
from flow.util import stats
import sys
import os
import traceback

import logging

LOG = logging.getLogger(__name__)

class CommandBase(object):
    __metaclass__ = ABCMeta

    default_logging_mode = 'default'

    @staticmethod
    def annotate_parser(parser):
        raise NotImplementedError

    @abstractmethod
    def __call__(self, parsed_arguments):
        raise NotImplementedError

def main():
    exit_code = 1
    try:
        commands = flow.configuration.load_commands()

        parser = flow.configuration.create_parser(commands)
        arguments = parser.parse_args(sys.argv[1:])

        logging_mode = arguments.logging_mode
        if logging_mode is None:
            logging_mode = arguments.command.default_logging_mode

        config = flow.configuration.load_config(arguments.config)

        flow.configuration.initialize_logging(config, logging_mode)
        flow.configuration.initialize_statsd(config)

        stats.increment_as_user('command', arguments.command_name)

        try:
            command_config = config['commands'][arguments.command_name]
            kwargs = dictionary_factory(**command_config)
            command = arguments.command(**kwargs)
            exit_code = command(arguments)
        except:
            LOG.exception('Command execution failed')
            os._exit(1)

    except SystemExit as e:
        exit_code = e.exit_code
    except:
        sys.stderr.write('Unhandled exception:\n')
        traceback.print_exc()
    finally:
        return exit_code
