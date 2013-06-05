import logging
import os


LOG = logging.getLogger(__name__)


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
