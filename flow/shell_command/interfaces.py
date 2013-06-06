from abc import ABCMeta, abstractmethod


class IShellCommand(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def submit(self, command_line, net_key=None, response_places=None,
            **executor_options):
        pass


class IShellCommandExecutor(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def execute(self, group_id, user_id, environment, working_directory,
            command_line, executor_data, callback_data, service_interfaces):
        pass
