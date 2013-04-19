from flow.configuration.settings.injector import setting
from flow.shell_command.messages import ShellCommandSubmitMessage
from flow.interfaces import IShellCommandExecutor, IBroker, IStorage
from flow.service_locator import ServiceLocator
from flow.petri import Token, SetTokenMessage
from injector import inject

import flow.interfaces
import logging


LOG = logging.getLogger(__name__)


@inject(executor=IShellCommandExecutor)
class ShellCommandSubmitMessageHandler(flow.interfaces.IHandler):
    message_class = ShellCommandSubmitMessage


@inject(queue_name=setting('shell_command.fork.queue'),
        exchange=setting('shell_command.fork.exchange'),
        response_routing_key=setting('shell_command.fork.response_routing_key'))
class ForkShellCommandMessageHandler(ShellCommandSubmitMessageHandler):
    def __call__(self, message):
        LOG.debug('ForkShellCommandSubmitMessageHandler got message')
        executor_options = getattr(message, 'executor_options', {})

        response_places = message.response_places
        net_key = message.net_key

        job_id, success = self.executor(message.command_line,
                net_key=net_key, response_places=response_places,
                **executor_options)

@inject(storage=IStorage,
        service_locator=ServiceLocator,
        queue_name=setting('shell_command.lsf.queue'),
        exchange=setting('shell_command.lsf.exchange'),
        response_routing_key=setting('shell_command.lsf.response_routing_key'))
class LSFShellCommandMessageHandler(ShellCommandSubmitMessageHandler):
    def __call__(self, message):
        LOG.debug('LsfShellCommandSubmitMessageHandler got message')
        executor_options = getattr(message, 'executor_options', {})

        response_places = message.response_places
        net_key = message.net_key

        job_id, success = self.executor(message.command_line,
                net_key=net_key, response_places=response_places,
                **executor_options)

        if success:
            response_place = response_places.get('post_dispatch_success')
        else:
            response_place = response_places.get('post_dispatch_failure')

        token = Token.create(self.storage, data={"pid": str(job_id)})

        orchestrator = self.service_locator['orchestrator']
        orchestrator.set_token(net_key=net_key, place_idx=response_place,
                token_key=token.key)

