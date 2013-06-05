from flow.shell_command.interfaces import IShellCommandExecutor
from flow.shell_command.lsf.executor import LSFExecutor

import injector

class LSFExecutorConfiguration(injector.Module):
    def configure(self, binder):
        binder.bind(IShellCommandExecutor, LSFExecutor)
