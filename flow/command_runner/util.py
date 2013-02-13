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

    saved_environment = dict(os.environ)
    os.environ.clear()
    os.environ.update(temporary_environment)

    yield

    os.environ.clear()
    os.environ.update(saved_environment)

@contextmanager
def seteuid(user_id):
    if user_id is not None:
        saved_user_id = os.geteuid()
        LOG.debug('Setting uid to 0, then to %d', user_id)
        os.seteuid(0)
        os.seteuid(user_id)

    yield

    if user_id is not None:
        os.seteuid(0)
        os.seteuid(saved_user_id)
        LOG.debug('uid reset to %d', saved_user_id)
