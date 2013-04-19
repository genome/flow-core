from flow.shell_command.executors.lsf import LSFExecutor

import flow.interfaces
import injector

class LSFExecutorConfiguration(injector.Module):
    def configure(self, binder):
        binder.bind(flow.interfaces.IShellCommandExecutor, LSFExecutor)
