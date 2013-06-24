from flow.configuration.settings.injector import setting
from flow.handler import Handler
from flow.interfaces import IServiceLocator
from flow.shell_command import resource
from flow.shell_command.execution_environment import ExecutionEnvironment
from flow.shell_command.interfaces import IShellCommandExecutor
from flow.shell_command.messages import ShellCommandSubmitMessage
from flow.util import environment as env_util
from injector import inject
from twisted.internet import defer

import logging


LOG = logging.getLogger(__name__)


@inject(executor=IShellCommandExecutor, service_interfaces=IServiceLocator,
        resource_definitions=setting('shell_command.resources'),
        default_environment=setting('shell_command.default_environment', {}),
        mandatory_environment=
            setting('shell_command.mandatory_environment', {}))
class ShellCommandSubmitMessageHandler(Handler):
    message_class = ShellCommandSubmitMessage

    def __init__(self):
        self.resource_types = resource.make_resource_types(
                self.resource_definitions)

    def assemble_environment(self, message):
        return env_util.merge_and_sanitize_environments(
                self.default_environment,
                message.get('environment', {}),
                self.mandatory_environment)

    def _handle_message(self, message):
        resources = resource.make_all_resource_objects(
                message.get('resources', {}), self.resource_types)

        execution_environment = ExecutionEnvironment(
                group_id=message['group_id'],
                user_id=message['user_id'],
                environment=self.assemble_environment(message),
                working_directory=message.get('working_directory', '/tmp')
        )

        return self.executor.execute(
                command_line=message['command_line'],
                execution_environment=execution_environment,
                callback_data=message.get('callback_data', {}),
                executor_data=message.get('executor_data', {}),
                resources=resources,
                service_interfaces=self.service_interfaces)



@inject(queue_name=setting('shell_command.fork.queue'),
        exchange=setting('shell_command.fork.exchange'),
        response_routing_key=setting('shell_command.fork.response_routing_key'))
class ForkShellCommandMessageHandler(ShellCommandSubmitMessageHandler):
    pass

@inject(queue_name=setting('shell_command.lsf.queue'),
        exchange=setting('shell_command.lsf.exchange'),
        response_routing_key=setting('shell_command.lsf.response_routing_key'))
class LSFShellCommandMessageHandler(ShellCommandSubmitMessageHandler):
    pass
