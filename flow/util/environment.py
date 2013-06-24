from contextlib import contextmanager
import logging
import os


LOG = logging.getLogger(__name__)


@contextmanager
def environment(environment_dicts):
    saved_environment = dict(os.environ.data)
    set_environment(*environment_dicts)

    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(saved_environment)

def merge_and_sanitize_environments(*environment_dicts):
    env = {}
    for ed in environment_dicts:
        for k, v in ed.iteritems():
            try:
                env[str(k)] = str(v)
            except UnicodeEncodeError:
                LOG.warn('Failed to convert unicode environment variable %s=%s',
                        k, v)
    return env

def set_environment(*environment_dicts):
    temporary_environment = dict()
    for env in environment_dicts:
        if env:
            temporary_environment.update(env)

    os.environ.clear()
    for key, value in temporary_environment.iteritems():
        try:
            os.environ[key] = value
        except UnicodeEncodeError:
            LOG.warn('Failed to update environment variable %s=%s... skipping',
                    key, value)
