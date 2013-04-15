from flow.factories import dictionary_factory
from flow.util import stats
import flow.configuration
import injector
import logging
import os
import sys
import traceback

LOG = logging.getLogger(__name__)

def main():
    exit_code = 1
    try:
        commands = flow.configuration.load_commands()

        parser = flow.configuration.create_parser(commands)
        arguments = parser.parse_args(sys.argv[1:])

        logging_mode = arguments.logging_mode
        if logging_mode is None:
            logging_mode = 'default'

        config = flow.configuration.load_config(arguments.config)

        flow.configuration.initialize_logging(config, logging_mode)
        flow.configuration.initialize_statsd(config)

        stats.increment_as_user('command', arguments.command_name)
        command_class = arguments.command

        try:
            i = injector.Injector(command_class.injector_modules)

            command = i.get(command_class)
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
