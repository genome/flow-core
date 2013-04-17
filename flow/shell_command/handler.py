from flow.shell_command.messages import ShellCommandSubmitMessage
from flow.interfaces import IShellCommandExecutor, IBroker, IStorage
from flow.petri import Token, SetTokenMessage
from injector import inject, Setting

import logging


LOG = logging.getLogger(__name__)


@inject(executor=IShellCommandExecutor, broker=IBroker, storage=IStorage)
class ShellCommandSubmitMessageHandler(object):
    message_class = ShellCommandSubmitMessage

    def __call__(self, message):
        LOG.debug('ShellCommandSubmitMessageHandler got message')
        executor_options = getattr(message, 'executor_options', {})

        response_places = message.response_places
        net_key = message.net_key

        self.set_token(net_key, response_places.get('pre_dispatch'))

        job_id, success = self.executor(message.command_line,
                net_key=net_key, response_places=response_places,
                **executor_options)

        if success:
            response_place = response_places.get('post_dispatch_success')
        else:
            response_place = response_places.get('post_dispatch_failure')

        self.set_token(net_key, response_place, data={"pid": str(job_id)})

    def set_token(self, net_key, place_idx, data=None):
        if place_idx is not None:
            token = Token.create(self.storage, data=data)
            response_message = SetTokenMessage(token_key=token.key,
                    net_key=net_key, place_idx=int(place_idx))
            self.broker.publish(self.exchange,
                    self.routing_key, response_message)

@inject(queue_name=Setting('shell_command.fork.queue'),
        exchange=Setting('shell_command.fork.exchange'),
        routing_key=Setting('shell_command.fork.routing_key'))
class ForkShellCommandMessageHandler(ShellCommandSubmitMessageHandler): pass

@inject(queue_name=Setting('shell_command.lsf.queue'),
        exchange=Setting('shell_command.lsf.exchange'),
        routing_key=Setting('shell_command.lsf.routing_key'))
class LSFShellCommandMessageHandler(ShellCommandSubmitMessageHandler): pass
