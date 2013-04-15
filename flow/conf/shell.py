from flow.command_runner.service_interface import CommandLineServiceInterface
import flow.interfaces
import injector

class LocalShellConfiguration(injector.Module):
    @injector.provides(flow.interfaces.ILocalShellCommand)
    @injector.inject(broker=flow.interfaces.IBroker,
            exchange=injector.Setting('shell.local.exchange'),
            submit_routing_key=injector.Setting('shell.local.submit_routing_key'))
    def provide_local_shell_command(self, **kwargs):
        return CommandLineServiceInterface(**kwargs)

class LSFConfiguration(injector.Module):
    @injector.provides(flow.interfaces.IGridShellCommand)
    @injector.inject(broker=flow.interfaces.IBroker,
            exchange=injector.Setting('shell.lsf.exchange'),
            submit_routing_key=injector.Setting('shell.lsf.submit_routing_key'))
    def provide_lsf_shell_command(self, **kwargs):
        return CommandLineServiceInterface(**kwargs)
