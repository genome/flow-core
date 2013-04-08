import logging
from flow.petri import Token, SetTokenMessage
from flow.command_runner.messages import CommandLineSubmitMessage

LOG = logging.getLogger(__name__)

class CommandLineSubmitMessageHandler(object):
    message_class = CommandLineSubmitMessage
    def __init__(self, executor=None, broker=None, storage=None,
            queue_name=None, exchange=None, routing_key=None):
        self.executor = executor
        self.broker = broker
        self.queue_name = queue_name
        self.storage = storage

        self.exchange = exchange
        self.routing_key = routing_key

    def __call__(self, message):
        LOG.debug('CommandLineSubmitMessageHandler got message')
        executor_options = getattr(message, 'executor_options', {})

        response_places = message.response_places
        net_key = message.net_key

        self.set_token(net_key, response_places.get('pre_dispatch'))

        pid, exit_code, signal = self.executor(message.command_line,
                net_key=net_key, response_places=response_places,
                **executor_options)

        if signal:
            raise RuntimeError('Child got signal, stop the show! '
                    '(note that SIGHUP often means an exception was '
                    'raised inside the child)')

        if exit_code:
            self.set_token(net_key, response_places.get('post_dispatch_failure'))
        else:
            self.set_token(net_key, response_places.get('post_dispatch_success'),
                    data={"pid": str(pid)})

    def set_token(self, net_key, place_idx, data=None):
        if place_idx is not None:
            token = Token.create(self.storage, data=data)
            response_message = SetTokenMessage(token_key=token.key,
                    net_key=net_key, place_idx=int(place_idx))
            self.broker.publish(self.exchange,
                    self.routing_key, response_message)
