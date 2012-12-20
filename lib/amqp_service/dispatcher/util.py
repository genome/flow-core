from contextlib import contextmanager
import os

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
