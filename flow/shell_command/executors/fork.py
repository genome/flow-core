from flow.shell_command import util
from flow.shell_command.executors.base import ExecutorBase

import logging
import os
import subprocess


LOG = logging.getLogger(__name__)


class ForkExecutor(ExecutorBase):
    def execute(self, command_line, net_key=None, response_places=None,
            working_directory=None, stdout=None, stderr=None,
            with_inputs=None, with_outputs=False, token_color=None,
            **kwargs):

        full_command_line = self._make_command_line(command_line,
                net_key=net_key, response_places=response_places,
                with_inputs=with_inputs, with_outputs=with_outputs,
                token_color=token_color)

        LOG.debug('working_directory = %s', working_directory)
        LOG.debug('PATH = %s', os.getenv('PATH'))

        stdout_fh = None
        stderr_fh = None
        try:
            if stdout:
                stdout_fh = open(util.join_path_if_rel(
                    working_directory, stdout), 'a')
            if stderr:
                stderr_fh = open(util.join_path_if_rel(
                    working_directory, stderr), 'a')

            LOG.debug('executing command %s', " ".join(full_command_line))
            exit_code = subprocess.call(full_command_line,
                    stdout=stdout_fh, stderr=stderr_fh,
                    cwd=working_directory)

        except OSError:
            LOG.exception('Executor got OSError')
            raise
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
