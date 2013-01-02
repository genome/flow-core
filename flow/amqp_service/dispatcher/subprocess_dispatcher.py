import logging
import os
import subprocess

from flow.amqp_service.dispatcher import util

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
                if stdout:
                    stdout_fh = open(stdout, 'a')
                else:
                    stdout_fh = None
                if stderr:
                    stderr_fh = open(stderr, 'a')
                else:
                    stderr_fh = None

                exit_code = subprocess.call(map(str, command_line),
                        stdout=stdout_fh, stderr=stderr_fh,
                        cwd=working_directory)
            except OSError as e:
                error_message = 'Dispatcher got error number (%d): %s' % (
                        e.errno, os.strerror(e.errno))
                LOG.error(error_message)
                raise RuntimeError(error_message)
            finally:
                if stdout_fh:
                    stdout_fh.close()
                if stderr_fh:
                    stderr_fh.close()

        if exit_code > 0:
            # XXX get error message
            LOG.debug('failed to execute subprocess job, exit_code = %d',
                    exit_code)
            return False, exit_code
        else:
            LOG.debug('succesfully executed subprocess job')
            return True, exit_code
