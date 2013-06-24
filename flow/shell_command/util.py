from flow import exit_codes
from flow.util.exit import exit_process

import logging
import os
import socket


LOG = logging.getLogger(__name__)


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
        exit_process(exit_codes.EXECUTE_SYSTEM_FAILURE)

    return parent_socket, child_socket


def fork_or_exit():
    try:
        pid = os.fork()
    except OSError:
        LOG.exception('Failed to fork, exitting')
        exit_process(exit_codes.EXECUTE_SYSTEM_FAILURE)

    return pid
