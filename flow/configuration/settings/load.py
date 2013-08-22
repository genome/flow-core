from flow.configuration.defaults import DEFAULT_FLOW_CONFIG_PATH
from flow.configuration.settings.cache import CacheSettings
from flow.configuration.settings.priority import PrioritySettings

import os
import yaml


def get_valid_config_dirs():
    config_path = os.environ.get('FLOW_CONFIG_PATH', DEFAULT_FLOW_CONFIG_PATH)
    config_dirs = [os.path.expandvars(d) for d in config_path.split(':')]

    return [d for d in reversed(config_dirs) if os.path.isdir(d)]


def base_config_name(config_dir):
    return os.path.join(config_dir, 'flow.yaml')

def command_config_name(config_dir, command_name):
    return os.path.join(config_dir, '%s.yaml' % command_name)


def get_config_file_paths(command_name):
    config_dirs = get_valid_config_dirs()
    results = []

    for config_dir in config_dirs:
        results.append(base_config_name(config_dir))

    for config_dir in config_dirs:
        results.append(command_config_name(config_dir, command_name))

    return filter(os.path.isfile, results)


def load_settings(command_name, parsed_arguments):
    settings = PrioritySettings()

    for config_file in get_config_file_paths(command_name):
        settings.append(load_settings_file(config_file))

    # environment variables?

    # cli arguments?
    #   is this only useful for logging?

    return settings


def load_settings_file(path):
    data = yaml.load(open(path))
    return CacheSettings(data)
