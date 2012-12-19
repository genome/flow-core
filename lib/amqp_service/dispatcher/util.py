from contextlib import contextmanager
import os

@contextmanager
def environment(environment_dicts):
    temporary_environment = dict()
    for env in environment_dicts:
        temporary_environment.update(env)

    saved_environment = dict(os.environ)
    os.environ.clear()
    os.environ.update(temporary_environment)

    yield

    os.environ.clear()
    os.environ.update(saved_environment)
