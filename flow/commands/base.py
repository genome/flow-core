from abc import *
import flow.configuration
from flow.factories import dictionary_factory
import traceback

class CommandBase(object):
    __metaclass__ = ABCMeta

    default_logging_mode = 'default'

    @staticmethod
    def annotate_parser(parser):
        raise NotImplementedError

    @abstractmethod
    def __call__(self, parsed_arguments):
        raise NotImplementedError

def run(argv, LOG):
    exit_code = 1
    try:
        commands = flow.configuration.load_commands()

        parser = flow.configuration.create_parser(commands)
        arguments = parser.parse_args(argv)

        logging_mode = arguments.logging_mode
        if logging_mode is None:
            logging_mode = arguments.command.default_logging_mode

        config = flow.configuration.load_config(arguments.config)

        logging_config = config['logging_configurations'][logging_mode]
        flow.configuration.setup_logging(logging_config)

        try:
            command_config = config['commands'][arguments.command_name]
            kwargs = dictionary_factory(**command_config)
            command = arguments.command(**kwargs)
            exit_code = command(arguments)
        except:
            LOG.exception('Command execution failed')

    except SystemExit as e:
        exit_code = e.exit_code
    except:
        sys.stderr.write('Unhandled exception:\n')
        traceback.print_exc()
    finally:
        return exit_code

