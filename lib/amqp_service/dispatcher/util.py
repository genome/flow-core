from contextlib import contextmanager
import os

@contextmanager
def environment(temporary_environment):
    saved_environment = dict(os.environ)
    os.environ.clear()
    os.environ.update(temporary_environment)

    yield

    os.environ.clear()
    os.environ.update(saved_environment)
