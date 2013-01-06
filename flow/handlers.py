import logging

LOG = logging.getLogger(__name__)

class RespondingHandler(object):
    def __init__(self, broker=None):
        self.broker = broker

    def on_message(self, message)
        response_routing_key, response_message = self.message_handler(message)
        self.broker.publish(response_routing_key, response_message)
