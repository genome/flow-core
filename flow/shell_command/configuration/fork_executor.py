from flow.shell_command.executors.fork import ForkExecutor
from flow.shell_command.interfaces import IShellCommandExecutor

import injector

class ForkExecutorConfiguration(injector.Module):
    def configure(self, binder):
        binder.bind(IShellCommandExecutor, ForkExecutor)
