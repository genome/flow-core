import os
import errno


def make_path_to(filename):
    if filename:
        mkdir_p(os.path.dirname(filename))

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

