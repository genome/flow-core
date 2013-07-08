from flow.configuration.settings.injector import setting
from flow.shell_command.fork import executor
from flow.shell_command.handler_base import ShellCommandSubmitMessageHandler
from injector import inject


@inject(executor=executor.ForkExecutor,
        queue_name=setting('shell_command.fork.queue'),
        exchange=setting('shell_command.fork.exchange'),
        response_routing_key=setting('shell_command.fork.response_routing_key'),
        default_environment=
            setting('shell_command.fork.default_environment', {}),
        mandatory_environment=
            setting('shell_command.fork.mandatory_environment', {}))
class ForkShellCommandMessageHandler(ShellCommandSubmitMessageHandler):
    pass
