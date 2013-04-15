from flow.configuration.defaults import DEFAULT_FLOW_CONFIG_PATH
from flow.configuration.settings.cache import CacheSettings
from flow.configuration.settings.priority import PrioritySettings

import os
import yaml

def get_valid_config_dirs():
    config_path = os.environ.get('FLOW_CONFIG_PATH', DEFAULT_FLOW_CONFIG_PATH)
    config_dirs = [os.path.expandvars(d) for d in config_path.split(':')]

    return [d for d in config_dirs if os.path.isdir(d)]


def load_settings(command_name, parsed_arguments):
    settings = PrioritySettings()

    for config_dir in get_valid_config_dirs():
        settings.extend(load_config_dir(config_dir, command_name))

    # environment variables?

    # cli arguments?
    #   is this only useful for logging?

    return settings


def load_config_dir(config_dir, command_name=None):
    results = [
        load_settings_file(os.path.join(config_dir, 'flow.yaml')),
        load_settings_file(
            os.path.join(config_dir, '%s.yaml' % command_name)),
    ]

    return [r for r in results if r]

def load_settings_file(path):
    if os.path.isfile(path):
        data = yaml.load(open(path))
        return CacheSettings(data)
