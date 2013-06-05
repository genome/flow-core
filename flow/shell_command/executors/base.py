from flow import exit_codes
from flow.configuration.settings.injector import setting
from flow.shell_command.interfaces import IShellCommandExecutor
from flow.util import environment as env_util
from injector import inject

import logging
import os
import socket


LOG = logging.getLogger(__name__)


@inject(wrapper=setting('shell_command.wrapper'),
        default_environment=setting('shell_command.default_environment', {}),
        mandatory_environment=
            setting('shell_command.mandatory_environment', {}))
class ExecutorBase(IShellCommandExecutor):
    def __call__(self, command_line, group_id=None, user_id=None,
            environment={}, **kwargs):
        parent_socket, child_socket = socketpair_or_exit()

        pid = fork_or_exit()
        if pid:  # Parent
            child_socket.close()
            job_id, exit_code = wait_for_child(parent_socket, pid)

            if exit_code == exit_codes.EXECUTE_SUCCESS:
                return job_id, True
            elif exit_code == exit_codes.EXECUTE_FAILURE:
                return job_id, False
            else:
                raise RuntimeError('Unknown exit code (%d) from child!'
                        % exit_code)

        else:  # Child
            parent_socket.close()

            set_gid_and_uid_or_exit(group_id, user_id)

            exit_code = self._child_execute(child_socket,
                    command_line, environment, kwargs)

            os._exit(exit_code)

    def _child_execute(self, child_socket, command_line, environment, kwargs):
        try:
            env_util.set_environment(self.default_environment,
                    environment, self.mandatory_environment)

            success, job_id = self.execute(command_line, **kwargs)

            if success:
                child_socket.send(str(job_id))
                child_socket.close()
                return exit_codes.EXECUTE_SUCCESS
            else:
                return exit_codes.EXECUTE_FAILURE

        except:
            LOG.exception('Executor raised exception, exitting.')
            return exit_codes.EXECUTE_ERROR

    def _make_command_line(self, command_line, net_key=None,
            response_places=None, with_inputs=None, with_outputs=None,
            token_color=None):

        cmdline = self.wrapper + [
            '-n', net_key,
            '-r', response_places['begin_execute'],
            '-s', response_places['execute_success'],
        ]
        if 'execute_failure' in response_places.keys():
            cmdline += ['-f', response_places['execute_failure']]

        if token_color is not None:
            cmdline += ["--token-color", str(token_color)]

        if with_inputs:
            cmdline += ["--with-inputs", with_inputs]

        if with_outputs:
            cmdline.append("--with-outputs")

        cmdline.append('--')
        cmdline += command_line

        return [str(x) for x in cmdline]


def wait_for_child(parent_socket, pid):
    child_pid, exit_status = os.waitpid(pid, 0)
    signal_number = 255 & exit_status
    exit_code = exit_status >> 8

    if signal_number:
        raise RuntimeError('Executor child got signal (%d), '
                'rejecting message' % signal_number)

    if exit_code == exit_codes.EXECUTE_SYSTEM_FAILURE:
        os._exit(exit_codes.EXECUTE_SYSTEM_FAILURE)
    elif exit_code == exit_codes.EXECUTE_ERROR:
        raise RuntimeError('Error in executor child process '
                '(exit code %d).  Rejecting message.' % exit_code)

    job_id = -1
    if not signal_number and exit_code == exit_codes.EXECUTE_SUCCESS:
        job_id = int(parent_socket.recv(64))

    parent_socket.close()

    return job_id, exit_code


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


def set_gid_and_uid_or_exit(group_id, user_id):
    if group_id is not None:
        try:
            LOG.debug('Setting group id to %d', group_id)
            os.setgid(group_id)
        except:
            LOG.exception('Failed to setgid from %d to %d',
                    os.getgid(), group_id)
            os._exit(exit_codes.EXECUTE_SYSTEM_FAILURE)

    if user_id is not None:
        try:
            LOG.debug('Setting user id to %d', user_id)
            os.setuid(user_id)
        except:
            LOG.exception('Failed to setuid from %d to %d',
                    os.getuid(), user_id)
            os._exit(exit_codes.EXECUTE_SYSTEM_FAILURE)
