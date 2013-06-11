from flow.configuration import defaults
from flow.configuration.commands import determine_command
from flow.configuration.inject.initialize import initialize_injector
from flow.configuration.metrics import initialize_metrics
from flow.configuration.parser import parse_arguments
from flow.configuration.settings.load import load_settings

import flow.exit_codes
import flow.util.stats
import logging.config
import pika
import sys
import traceback


LOG = logging.getLogger(__name__)


def main():
    try:
        exit_code = naked_main()

    except SystemExit as e:
        exit_code = e.code
    except:
        sys.stderr.write('Unhandled exception:\n')
        traceback.print_exc()
        exit_code = flow.exit_codes.UNKNOWN_ERROR

    return exit_code


def naked_main():
    command_class = determine_command()
    parsed_args = parse_arguments(command_class)

    settings = load_settings(command_class.name, parsed_args)

    logging.config.dictConfig(settings.get('logging',
        defaults.DEFAULT_LOGGING_CONFIG))
    initialize_metrics(settings)

    injector = initialize_injector(settings, command_class)

    flow.util.stats.increment_as_user('command', command_class.name)

    # XXX Hack to get the command to show up in the rabbitmq admin interface
    pika.connection.PRODUCT = command_class.name

    try:
        LOG.info('Loading command (%s)', command_class.name)
        command = injector.get(command_class)
    except:
        LOG.exception('Could not instantiate command object.')
        return flow.exit_codes.EXECUTE_ERROR

    try:
        exit_code = command.execute(parsed_args)
    except:
        LOG.exception('Command execution failed')
        return flow.exit_codes.EXECUTE_FAILURE

    return exit_code
