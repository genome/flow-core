from flow.configuration.settings.injector import setting
from flow.handler import Handler
from flow.interfaces import IServiceLocator
from flow.shell_command import resource_types
from flow.shell_command.execution_environment import ExecutionEnvironment
from flow.shell_command.messages import ShellCommandSubmitMessage
from flow.util import environment as env_util
from injector import inject
from twisted.internet import defer

import logging


LOG = logging.getLogger(__name__)


@inject(service_interfaces=IServiceLocator,
        resource_type_definitions=setting('shell_command.resource_types'))
class ShellCommandSubmitMessageHandler(Handler):
    message_class = ShellCommandSubmitMessage

    def __init__(self):
        self.resource_types = resource_types.make_resource_types(
                self.resource_type_definitions)

    def assemble_environment(self, message):
        return env_util.merge_and_sanitize_environments(
                self.default_environment,
                message.get('environment', {}),
                self.mandatory_environment)

    def _handle_message(self, message):
        resources = resource_types.make_all_resource_objects(
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
