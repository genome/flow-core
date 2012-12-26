try:
    from logging.config import dictConfig
except ImportError:
    from compat.dictconfig import dictConfig

import argparse
import yaml

def setup_logging(configuration_filename):
    # XXX Should probably use templating, then have yaml parse it as a dict
    # That way we can easily set filenames and log levels, etc.
    with open(configuration_filename) as f:
        configuration_dict = yaml.load(f)

    dictConfig(configuration_dict)


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--logging_configuration',
            default='config/console_color.yaml')

    return parser.parse_args()
