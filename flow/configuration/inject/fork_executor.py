from flow.shell_command.executors.fork import ForkExecutor

import flow.interfaces
import injector

class ForkExecutorConfiguration(injector.Module):
    def configure(self, binder):
        binder.bind(flow.interfaces.IShellCommandExecutor, ForkExecutor)
