import logging
import os
import subprocess

from amqp_service.dispatcher import util

LOG = logging.getLogger(__name__)

class SubprocessDispatcher(object):
    def __init__(self, default_environment={}, manditory_environment={}):
        self.default_environment = default_environment
        self.manditory_environment = manditory_environment

    def launch_job(self, command_line, working_directory=None,
            environment={}, stdout=None, stderr=None, **kwargs):

        with util.environment([self.default_environment, environment,
                               self.manditory_environment]):
            LOG.debug('executing subprocess using command_line: %s',
                    command_line)
            try:
                exit_code = subprocess.call(map(str, command_line),
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
