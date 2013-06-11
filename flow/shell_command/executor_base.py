from flow.configuration.settings.injector import setting
from flow.shell_command import util
from flow.shell_command.interfaces import IShellCommandExecutor
from flow.util import environment as env_util
from flow.util.exit import exit_process
from injector import inject
from twisted.internet import defer

import abc
import logging
import os


LOG = logging.getLogger(__name__)



@inject(default_environment=setting('shell_command.default_environment', {}),
        mandatory_environment=
            setting('shell_command.mandatory_environment', {}))
class ExecutorBase(IShellCommandExecutor):
    @abc.abstractmethod
    def execute_command_line(self, job_id_callback, command_line,
            executor_data, resources):
        pass

    def on_job_id(self, job_id, callback_data, service_interfaces):
        return defer.succeed(None)

    def on_failure(self, exit_code, callback_data, service_interfaces):
        return defer.succeed(None)

    def on_signal(self, signal_number, callback_data, service_interfaces):
        raise RuntimeError('Child received signal (%d)' % signal_number)

    def on_success(self, callback_data, service_interfaces):
        return defer.succeed(None)


    def execute(self, group_id, user_id, environment, working_directory,
            command_line, executor_data, callback_data,
            resources, service_interfaces):
        deferreds = []

        child_pid = self.fork_child(deferreds, group_id, user_id, environment,
                working_directory, command_line, executor_data, callback_data,
                resources, service_interfaces)
        exit_code, signal_number = util.wait_for_pid(child_pid)

        if signal_number > 0:
            deferreds.append(self.on_signal(
                signal_number, callback_data, service_interfaces))
        elif exit_code > 0:
            deferreds.append(self.on_failure(
                exit_code, callback_data, service_interfaces))
        else:
            deferreds.append(self.on_success(callback_data, service_interfaces))

        return defer.gatherResults(deferreds, consumeErrors=True)

    def fork_child(self, deferreds, group_id, user_id, environment,
            working_directory, command_line, executor_data, callback_data,
            resources, service_interfaces):
        parent_socket, child_socket = util.socketpair_or_exit()

        child_pid = util.fork_or_exit()
        if child_pid == 0:
            parent_socket.close()
            child_exit_code = self._child(child_socket, group_id, user_id,
                    environment, working_directory, command_line,
                    executor_data, resources)
            exit_process(child_exit_code)

        else:
            child_socket.close()
            try:
                job_id = get_job_id(parent_socket)
                if job_id is not None:
                    deferreds.append(self.on_job_id(
                        job_id, callback_data, service_interfaces))

            except:
                LOG.exception('Error in parent, killing child')
                os.kill(child_pid, 9)
                raise
            finally:
                parent_socket.close()

        return child_pid

    def _child(self, send_socket, group_id, user_id, environment,
            working_directory, command_line, executor_data, resources):
        def send_job_id(job_id):
            send_socket.send(str(job_id))
            send_socket.close()

        util.set_gid_and_uid_or_exit(group_id, user_id)
        env_util.set_environment(self.default_environment,
                environment, self.mandatory_environment)
        try:
            os.chdir(working_directory)
        except OSError:
            os.kill(os.getpid(), 9)

        try:
            exit_code = self.execute_command_line(command_line=command_line,
                    job_id_callback=send_job_id, executor_data=executor_data,
                    resources=resources)
        except:
            LOG.exception('Exception caught in ShellCommand service child')
            os.kill(os.getpid(), 9)

        return exit_code


def get_job_id(recv_socket):
    data = recv_socket.recv(64)
    if data != '':
        return data
    else:
        return None


def send_message(place_name, callback_data,
        service_interfaces, token_data=None):
    net_key = callback_data['net_key']
    color = callback_data['color']
    color_group_idx = callback_data['color_group_idx']
    place_idx = callback_data[place_name]

    return service_interfaces['orchestrator'].create_token(
            net_key=net_key, place_idx=place_idx, color=color,
            color_group_idx=color_group_idx, data=token_data)
