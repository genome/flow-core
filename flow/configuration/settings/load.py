from flow.configuration.settings.cache import CacheSettings
from flow.configuration.settings.priority import PrioritySettings

import os
import yaml

_TEST_CONFIG = {
    'amqp.hostname': 'vmpool82',
    'amqp.port': 5672,
    'amqp.vhost': 'flow',
    'redis.host': 'vmpool83',
    'redis.port': 6379,
#    'redis.unix_socket_path': None,

    'shell.local.exchange': 'asdf',
    'shell.local.submit_routing_key': 'asdf',
    'shell.lsf.exchange': 'asdf',
    'shell.lsf.submit_routing_key': 'asdf',

    'shell.queue': 'fork_submit',
    # Concept of general callbacks could eliminate need for exchange/routing_key
    'shell.exchange': 'flow',
    'shell.routing_key': 'asdf',
    'shell.wrapper': ['a', 'b', 'c'],

    'send_token.exchange': 'flow',
    'send_token.routing_key': 'asdf',
}

_DEFAULT_FLOW_CONFIG_PATH = '/etc/flow:$HOME/.flow'

def get_valid_config_dirs():
    config_path = os.environ.get('FLOW_CONFIG_PATH', _DEFAULT_FLOW_CONFIG_PATH)
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
