import logging
import os
import subprocess

from amqp_service.dispatcher import util

LOG = logging.getLogger(__name__)

class SubprocessDispatcher(object):
    def launch_job(self, command, arguments=[], environment={},
            stdout=None, stderr=None, **kwargs):

        command_list = [command]
        command_list.extend(arguments)

        with util.environment(environment):
            LOG.debug('executing subprocess using command_list: %s',
                    command_list)
            try:
                exit_code = subprocess.call(command_list,
                        stdout=stdout, stderr=stderr)
            except OSError as e:
                error_message = 'Dispatcher got error number (%d): %s' % (
                        e.errno, os.strerror(e.errno))
                LOG.error(error_message)
                raise RuntimeError(error_message)

        if exit_code > 0:
            # XXX get error message
            LOG.debug('failed to execute subprocess job, exit_code = %d',
                    exit_code)
            return False, exit_code
        else:
            LOG.debug('succesfully executed subprocess job')
            return True, exit_code
