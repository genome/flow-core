import abc
import logging
import os
import signal
import socket

from flow.util import environment as env_util
from flow import exit_codes

LOG = logging.getLogger(__name__)


class ExecutorBase(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def execute(self, command_line, **kwargs):
        pass

    def __init__(self, wrapper=None, default_environment={},
            mandatory_environment={}):

        self.wrapper = wrapper
        self.default_environment = default_environment
        self.mandatory_environment = mandatory_environment

    def __call__(self, command_line, user_id=None, environment={}, **kwargs):
        parent_socket, child_socket = socketpair_or_exit()

        pid = fork_or_exit()
        if pid:  # Parent
            child_socket.close()
            return self._wait_for_child(parent_socket, pid)

        else:  # Child
            parent_socket.close()

            if user_id is not None:
                try:
                    LOG.debug('Setting user id to %d', user_id)
                    os.setuid(user_id)
                except:
                    LOG.exception('Failed to setuid')
                    os._exit(exit_codes.EXECUTE_SYSTEM_FAILURE)

            exit_code = self._child_execute(child_socket,
                    command_line, environment, kwargs)

            os._exit(exit_code)

    def _wait_for_child(self, parent_socket, pid):
        child_pid, exit_status = os.waitpid(pid, 0)
        signal_number = 255 & exit_status
        exit_code = exit_status >> 8

        if exit_code == exit_codes.EXECUTE_SYSTEM_FAILURE:
            os._exit(exit_codes.EXECUTE_SYSTEM_FAILURE)

        job_id = -1
        if not signal_number and exit_code == exit_codes.EXECUTE_SUCCESS:
            job_id = int(parent_socket.recv(64))

        parent_socket.close()

        return job_id, exit_code, signal_number

    def _child_execute(self, socket, command_line, environment, kwargs):
        try:
            env_util.set_environment(self.default_environment,
                    environment, self.mandatory_environment)

            success, job_id = self.execute(command_line, **kwargs)

            if success:
                socket.send(str(job_id))
                socket.close()
                return exit_codes.EXECUTE_SUCCESS

            return exit_codes.EXECUTE_FAILURE

        except:
            LOG.exception('Executor raised exception, exitting.')
            return exit_codes.EXECUTE_ERROR

    def _make_command_line(self, command_line, net_key=None,
            response_places=None, with_inputs=None, with_outputs=None):

        cmdline = self.wrapper + [
            '-n', net_key,
            '-r', response_places['begin_execute'],
            '-s', response_places['execute_success'],
        ]
        if 'execute_failure' in response_places.keys():
            cmdline += ['-f', response_places['execute_failure']]

        if with_inputs:
            cmdline += ["--with-inputs", with_inputs]

        if with_outputs:
            cmdline.append("--with-outputs")

        cmdline.append('--')
        cmdline += command_line

        return [str(x) for x in cmdline]


def socketpair_or_exit():
    try:
        parent_socket, child_socket = socket.socketpair()
    except:
        LOG.exception('Failed to create socket pair, exitting')
        os._exit(exit_codes.EXECUTE_SYSTEM_FAILURE)

    return parent_socket, child_socket


def fork_or_exit():
    try:
        pid = os.fork()
    except:
        LOG.exception('Failed to fork, exitting')
        os._exit(exit_codes.EXECUTE_SYSTEM_FAILURE)

    return pid
