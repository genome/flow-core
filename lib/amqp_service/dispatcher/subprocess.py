import logging
import subprocess

from amqp_service.dispatcher import util

LOG = logging.getLogger(__name__)

class SubprocessDispatcher(object):
    def launch_job(self, command, arguments=[],
            wrapper=None, wrapper_args=[], env={},
            stdout=None, stderr=None):

        command_list = []
        if wrapper:
            command_list.append(wrapper)
            command_list.extend(wrapper_args)

        command_list.append(command)
        command_list.extend(arg)

        with util.environment(env):
            exit_code = subprocess.call(command_list,
                    stdout=stdout, stderr=stderr)

        if exit_code > 0:
            return False, None
        else:
            return True, None
