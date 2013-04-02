from contextlib import contextmanager
import logging
import os

LOG = logging.getLogger(__name__)

@contextmanager
def environment(environment_dicts):
    temporary_environment = dict()
    for env in environment_dicts:
        temporary_environment.update(env)

    for k, v in temporary_environment.iteritems():
        if v is None:
            temporary_environment[k] = ''

    saved_environment = dict(os.environ.data)
    os.environ.clear()

    for key, value in temporary_environment.iteritems():
        try:
            os.environ[key] = value
        except UnicodeEncodeError:
            LOG.warn('Failed to update environment variable %s=%s... skipping',
                    key, value)
            pass

    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(saved_environment)
