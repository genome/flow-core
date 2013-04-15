from flow.command_runner.executors.local import SubprocessExecutor

import flow.interfaces
import injector

class ForkExecutorConfiguration(injector.Module):
    def configure(self, binder):
        binder.bind(flow.interfaces.IShellCommandExecutor, SubprocessExecutor)
