from flow import exit_codes
from flow.util.exit import exit_process

import abc
import os
import logging


LOG = logging.getLogger(__name__)


class ExecutionEnvironmentBase(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def enter(self):
        pass


class ExecutionEnvironment(ExecutionEnvironmentBase):
    def __init__(self, user_id, group_id, environment, working_directory,
            groups=None, umask=None):
        self.user_id = user_id
        self.group_id = group_id
        self.groups = groups
        self.umask = umask
        self.environment = environment
        self.working_directory = working_directory

    def enter(self):
        try:
            self.set_permissions()
            if self.umask:
                os.umask(self.umask)

            os.chdir(self.working_directory)

            os.environ.clear()
            os.environ.update(self.environment)

        except OSError:
            LOG.exception('Failed to enter execution environment:\n'
                    'group_id = %s, user_id = %s, working_directory = %s\n'
                    'environment:\n%s', self.group_id, self.user_id,
                        self.working_directory, self.environment)
            exit_process(exit_codes.EXECUTE_SYSTEM_FAILURE)

    def set_permissions(self):
        os.setgid(self.group_id)
        self.set_groups()
        os.setuid(self.user_id)

    def set_groups(self):
        if self.groups:
            if os.getuid() == 0:
                os.setgroups(self.groups)
            else:
                LOG.warning(
                'Failed to set groups to %r while running as %d (must be root)',
                    self.groups, os.getuid())


class NullExecutionEnvironment(ExecutionEnvironmentBase):
    def enter(self):
        pass
