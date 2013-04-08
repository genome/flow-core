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

def set_environment(*environment_dicts):
    temporary_environment = dict()
    for env in environment_dicts:
        temporary_environment.update(env)

    os.environ.clear()
    for key, value in temporary_environment.iteritems():
        try:
            os.environ[key] = value
        except UnicodeEncodeError:
            LOG.warn('Failed to update environment variable %s=%s... skipping',
                    key, value)
            pass
