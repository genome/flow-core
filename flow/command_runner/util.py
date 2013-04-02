from contextlib import contextmanager
import logging
import os

LOG = logging.getLogger(__name__)

@contextmanager
def seteuid(user_id):
    if user_id is not None:
        saved_user_id = os.geteuid()
        if user_id != saved_user_id:
            LOG.debug('Setting uid to 0, then to %d', user_id)
            os.seteuid(0)
            os.seteuid(user_id)
        else:
            LOG.debug('Uid is already %d, not changing', user_id)

    try:
        yield
    finally:
        if user_id is not None:
            if saved_user_id != os.geteuid():
                os.seteuid(0)
                os.seteuid(saved_user_id)
                LOG.debug('uid reset to %d', saved_user_id)
            else:
                LOG.debug('Uid is already %d, not changing', user_id)


def join_path_if_rel(*path_components):
    try:
        final_path_component = path_components[-1]
    except IndexError:
        raise RuntimeError('Not enough arguments given.')

    try:
        final_pc_is_abs = os.path.isabs(final_path_component)
    except AttributeError:
        raise RuntimeError('Could not determine whether final path component '
                           'was absolute path (probably not a string): %s' %
                ' + '.join(['(%r)' % pc for pc in path_components]))

    if not final_pc_is_abs:
        try:
            return os.path.join(*path_components)
        except AttributeError:
            raise RuntimeError('Failed to join path components '
                               '(probably non-string components): %s' %
                    ' + '.join(['(%r)' % pc for pc in path_components]))

    return final_path_component
