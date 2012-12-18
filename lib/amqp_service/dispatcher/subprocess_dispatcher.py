import logging
import subprocess

from amqp_service.dispatcher import util

LOG = logging.getLogger(__name__)

class SubprocessDispatcher(object):
    def launch_job(self, command, arguments=[],
            wrapper=None, wrapper_arguments=[], environment={},
            stdout=None, stderr=None, **kwargs):

        command_list = []
        if wrapper:
            command_list.append(wrapper)
            command_list.extend(wrapper_arguments)

        command_list.append(command)
        command_list.extend(arguments)

        with util.environment(environment):
            LOG.debug('executing subprocess using command_list: %s',
                    command_list)
            exit_code = subprocess.call(command_list,
                    stdout=stdout, stderr=stderr)

        if exit_code > 0:
            # XXX get error message
            LOG.debug('failed to execute subprocess job, exit_code = %d',
                    exit_code)
            return False, exit_code
        else:
            LOG.debug('succesfully executed subprocess job')
            return True, exit_code
