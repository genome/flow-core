import logging
import os
import socket


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


def wait_for_pid(pid):
    _, exit_status = os.waitpid(pid, 0)
    signal_number = 255 & exit_status
    exit_code = exit_status >> 8

    return exit_code, signal_number


def socketpair_or_exit():
    try:
        parent_socket, child_socket = socket.socketpair()
    except socket.error:
        LOG.exception('Failed to create socket pair, exitting')
        os._exit(exit_codes.EXECUTE_SYSTEM_FAILURE)

    return parent_socket, child_socket


def fork_or_exit():
    try:
        pid = os.fork()
    except OSError:
        LOG.exception('Failed to fork, exitting')
        os._exit(exit_codes.EXECUTE_SYSTEM_FAILURE)

    return pid


def set_gid_and_uid_or_exit(group_id, user_id):
    if group_id is not None:
        try:
            LOG.debug('Setting group id to %d', group_id)
            os.setgid(group_id)
        except OSError:
            LOG.exception('Failed to setgid from %d to %d',
                    os.getgid(), group_id)
            os._exit(exit_codes.EXECUTE_SYSTEM_FAILURE)

    if user_id is not None:
        try:
            LOG.debug('Setting user id to %d', user_id)
            os.setuid(user_id)
        except OSError:
            LOG.exception('Failed to setuid from %d to %d',
                    os.getuid(), user_id)
            os._exit(exit_codes.EXECUTE_SYSTEM_FAILURE)
