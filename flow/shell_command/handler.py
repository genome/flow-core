from flow.configuration.settings.injector import setting
from flow.handler import Handler
from flow.interfaces import IServiceLocator
from flow.service_locator import ServiceLocator
from flow.shell_command.interfaces import IShellCommandExecutor
from flow.shell_command.messages import ShellCommandSubmitMessage
from flow.shell_command import resource
from injector import inject
from twisted.internet import defer

import logging


LOG = logging.getLogger(__name__)


@inject(executor=IShellCommandExecutor, service_interfaces=IServiceLocator,
        resource_definitions=setting('shell_command.resources'))
class ShellCommandSubmitMessageHandler(Handler):
    message_class = ShellCommandSubmitMessage

    def __init__(self):
        self.resource_types = resource.make_resource_types(
                self.resource_definitions)

    def _handle_message(self, message):
        resources = resource.make_all_resource_objects(
                message.get('resources', {}), self.resource_types)

        return self.executor.execute(
                command_line=message['command_line'],
                group_id=message['group_id'],
                user_id=message['user_id'],
                callback_data=message.get('callback_data', {}),
                environment=message.get('environment', {}),
                executor_data=message.get('executor_data', {}),
                working_directory=message.get('working_directory', '/tmp'),
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
