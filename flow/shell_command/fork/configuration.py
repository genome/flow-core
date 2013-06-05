from flow.shell_command.fork.executor import ForkExecutor
from flow.shell_command.interfaces import IShellCommandExecutor

import injector

class ForkExecutorConfiguration(injector.Module):
    def configure(self, binder):
        binder.bind(IShellCommandExecutor, ForkExecutor)
