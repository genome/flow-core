import argparse
import yaml

try:
    from logging.config import dictConfig
except ImportError:
    from flow.compat.dictconfig import dictConfig


def load_config(configuration_filename):
    with open(configuration_filename) as f:
        configuration_dict = yaml.load(f)

    return configuration_dict


def setup_logging(logging_dict):
    dictConfig(logging_dict)


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default=None, help='Configuration file')
    parser.add_argument('--logging-mode', default='default',
            help='Logging configuration to use')
    parser.add_argument('executable_name')
    parser.add_argument('args', nargs='*', default=[],
            help='Addition options for the command')

    return parser.parse_args()
