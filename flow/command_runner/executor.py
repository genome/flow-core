import abc
import logging
import os
import signal
import socket

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

    def __call__(self, command_line, user_id=None, **kwargs):
        parent_socket, child_socket = socketpair_or_exit()

        pid = fork_or_exit()
        if pid:  # Parent
            child_socket.close()
            return self._wait_for_child(parent_socket, pid)

        else:  # Child
            parent_socket.close()

            if user_id is not None:
                os.setuid(user_id)

            try:
                exit_code = self._child_execute(
                        child_socket, command_line, kwargs)
            except:
                LOG.exception('Executor raised exception, exitting with signal.')
                os.kill(os.getpid(), signal.SIGHUP)

            os._exit(exit_code)

    def _wait_for_child(self, parent_socket, pid):
        child_pid, exit_status = os.waitpid(pid, 0)
        signal_number = 255 & exit_status
        exit_code = exit_status >> 8

        job_id = -1
        if not signal_number and exit_code == 0:
            job_id = int(parent_socket.recv(64))

        parent_socket.close()
        return job_id, exit_code, signal_number

    def _child_execute(self, socket, command_line, kwargs):
        success, job_id = self.execute(command_line, **kwargs)
        if success:
            socket.send(str(job_id))
            socket.close()
            return 0
        return 1

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


def socketpair_or_exit(failure_exit_code=1):
    try:
        parent_socket, child_socket = socket.socketpair()
    except:
        LOG.exception('Failed to create socket pair, exitting')
        os._exit(failure_exit_code)

    return parent_socket, child_socket


def fork_or_exit(failure_exit_code=1):
    try:
        pid = os.fork()
    except:
        LOG.exception('Failed to fork, exitting')
        os._exit(failure_exit_code)

    return pid
