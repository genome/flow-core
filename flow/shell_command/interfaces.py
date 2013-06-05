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
    def execute(self, command_line, **kwargs):
        pass

    @abstractmethod
    def __call__(self, command_line, group_id=None, user_id=None,
            environment={}, **kwargs):
        pass
