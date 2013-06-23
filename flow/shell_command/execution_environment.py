from flow.shell_command import util
from flow.util import environment as env_util

import abc
import os


class ExecutionEnvironmentBase(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def enter(self, default_environment, mandatory_environment):
        pass


class ExecutionEnvironment(ExecutionEnvironmentBase):
    def __init__(self, user_id, group_id, environment, working_directory):
        self.user_id = user_id
        self.group_id = group_id
        self.environment = environment
        self.working_directory = working_directory

    def enter(self, default_environment, mandatory_environment):
        util.set_gid_and_uid_or_exit(self.group_id,
                self.user_id)
        env_util.set_environment(default_environment,
                self.environment, mandatory_environment)
        try:
            os.chdir(self.working_directory)
        except OSError:
            os.kill(os.getpid(), 9)


class NullExecutionEnvironment(ExecutionEnvironmentBase):
    def enter(self, default_environment, mandatory_environment):
        pass
