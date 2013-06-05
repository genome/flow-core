from flow.configuration.settings.injector import setting
from flow.handler import Handler
from flow.interfaces import IShellCommandExecutor
from flow.service_locator import ServiceLocator
from flow.shell_command.messages import ShellCommandSubmitMessage
from injector import inject
from twisted.internet import defer

import logging


LOG = logging.getLogger(__name__)


@inject(executor=IShellCommandExecutor)
class ShellCommandSubmitMessageHandler(Handler):
    message_class = ShellCommandSubmitMessage


@inject(queue_name=setting('shell_command.fork.queue'),
        exchange=setting('shell_command.fork.exchange'),
        response_routing_key=setting('shell_command.fork.response_routing_key'))
class ForkShellCommandMessageHandler(ShellCommandSubmitMessageHandler):
    def _handle_message(self, message):
        LOG.debug('ForkShellCommandSubmitMessageHandler got message')
        executor_options = getattr(message, 'executor_options', {})

        response_places = message.response_places
        net_key = message.net_key
        token_color = getattr(message, "token_color", None)

        self.executor(message.command_line,
                net_key=net_key, response_places=response_places,
                token_color=token_color, **executor_options)
        return defer.succeed(True)

@inject(service_locator=ServiceLocator,
        queue_name=setting('shell_command.lsf.queue'),
        exchange=setting('shell_command.lsf.exchange'),
        response_routing_key=setting('shell_command.lsf.response_routing_key'))
class LSFShellCommandMessageHandler(ShellCommandSubmitMessageHandler):
    def _handle_message(self, message):
        LOG.debug('LsfShellCommandSubmitMessageHandler got message')
        executor_options = getattr(message, 'executor_options', {})

        response_places = message.response_places
        net_key = message.net_key
        token_color = getattr(message, "token_color", None)

        job_id, success = self.executor(message.command_line,
                net_key=net_key, response_places=response_places,
                token_color=token_color, **executor_options)

        if success:
            response_place = response_places.get('post_dispatch_success')
        else:
            response_place = response_places.get('post_dispatch_failure')

        orchestrator = self.service_locator['orchestrator']
        return orchestrator.create_token(net_key=net_key,
                place_idx=response_place, data={"pid": str(job_id)},
                token_color=token_color)
