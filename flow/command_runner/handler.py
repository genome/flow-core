import logging
from flow.petri.safenet import Token, SetTokenMessage

LOG = logging.getLogger(__name__)

class CommandLineSubmitMessageHandler(object):
    def __init__(self, executor=None, broker=None, storage=None,
            queue_name=None, routing_key=None):
        self.executor = executor
        self.broker = broker
        self.queue_name = queue_name
        self.storage = storage

        self.routing_key = routing_key

    def __call__(self, message):
        LOG.debug('CommandLineSubmitMessageHandler got message')
        executor_options = getattr(message, 'executor_options', {})

        response_places = message.response_places
        net_key = message.net_key

        place_idx = response_places['dispatch_failure']
        try:
            success, executor_result = self.executor(message.command_line,
                    net_key=net_key, response_places=response_places,
                    **executor_options)

            if success:
                place_idx = response_places['dispatch_success']

        except RuntimeError:
            LOG.exception('Got unhandled exception')

        token = Token.create(self.storage)
        response_message = SetTokenMessage(token_key=token.key,
                net_key=net_key, place_idx=place_idx)
        self.broker.publish(self.routing_key, response_message)
