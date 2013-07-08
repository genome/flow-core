from flow.configuration.settings.injector import setting
from flow.shell_command.handler_base import ShellCommandSubmitMessageHandler
from flow.shell_command.lsf import executor
from injector import inject


@inject(executor=executor.LSFExecutor,
        queue_name=setting('shell_command.lsf.queue'),
        exchange=setting('shell_command.lsf.exchange'),
        response_routing_key=setting('shell_command.lsf.response_routing_key'),
        default_environment=
            setting('shell_command.lsf.default_environment', {}),
        mandatory_environment=
            setting('shell_command.lsf.mandatory_environment', {}))
class LSFShellCommandMessageHandler(ShellCommandSubmitMessageHandler):
    pass
