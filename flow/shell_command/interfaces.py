from abc import ABCMeta, abstractmethod


class IShellCommand(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def submit(self, command_line, group_id, user_id, callback_data=None,
            environment=None, executor_data=None, resources=None,
            working_directory=None):
        pass


class IShellCommandExecutor(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def execute(self, execution_environment,
            command_line, executor_data, callback_data,
            resources, service_interfaces):
        pass
