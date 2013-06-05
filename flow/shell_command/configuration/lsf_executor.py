from flow.shell_command.executors.lsf import LSFExecutor
from flow.shell_command.interfaces import IShellCommandExecutor

import injector

class LSFExecutorConfiguration(injector.Module):
    def configure(self, binder):
        binder.bind(IShellCommandExecutor, LSFExecutor)
