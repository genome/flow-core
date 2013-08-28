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
    def __init__(self, user_id, group_id, groups, umask, environment, working_directory):
        self.user_id = user_id
        self.group_id = group_id
        self.groups = groups
        self.umask = umask
        self.environment = environment
        self.working_directory = working_directory

    def enter(self):
        try:
            os.setgid(self.group_id)
            os.setuid(self.user_id)
            os.setgroups(self.groups)
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


class NullExecutionEnvironment(ExecutionEnvironmentBase):
    def enter(self):
        pass
