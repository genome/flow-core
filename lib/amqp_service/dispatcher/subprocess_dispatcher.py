import logging
import subprocess

from amqp_service.dispatcher import util

LOG = logging.getLogger(__name__)

class SubprocessDispatcher(object):
    def launch_job(self, command, arguments=[],
            wrapper=None, wrapper_arguments=[], environment={},
            stdout=None, stderr=None):

        command_list = []
        if wrapper:
            command_list.append(wrapper)
            command_list.extend(wrapper_arguments)

        command_list.append(command)
        command_list.extend(arguments)

        with util.environment(environment):
            exit_code = subprocess.call(command_list,
                    stdout=stdout, stderr=stderr)

        if exit_code > 0:
            return False, exit_code
        else:
            return True, exit_code
