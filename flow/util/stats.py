import getpass
import statsd

import logging

LOG = logging.getLogger(__name__)

def increment_as_user(*label_components):
    try:
        statsd.increment(assemble_label(label_components, getpass.getuser()))
        statsd.increment(assemble_label(label_components, 'total'))
    except:
        LOG.exception('failed to increment as user %s', label_components)

def increment(*args, **kwargs):
    try:
        statsd.increment(*args, **kwargs)
    except:
        LOG.exception('failed to increment args=%s, kwargs=%s', args, kwargs)

def create_timer(name):
    return statsd.StatsdTimer(name)

def assemble_label(rest, tail):
    lc = list(rest) + [tail]
    return '.'.join(lc)
