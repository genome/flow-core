import logging
from flow.protocol import codec

LOG = logging.getLogger(__name__)

class AmqpBroker(object):
    def __init__(self, exchange_manager=None):
        self.exchange_manager = exchange_manager

    def publish(self, routing_key, message):
        encoded_message = codec.encode(message)
        self.exchange_manager.publish(routing_key=routing_key,
                message=encoded_message)

class AmqpListener(object):
    def __init__(self, delivery_callback=None):
        self.delivery_callback = delivery_callback

    def on_message(self, properties, encoded_message,
            ack_callback, reject_callback):
        try:
            message = codec.decode(encoded_message)
        except InvalidMessageError as e:
            LOG.exception('Invalid message.  Properties = %s, message = %s',
                    properties, encoded_message)
            reject_callback()

        try:
            self.delivery_callback(message)
            ack_callback()
        except:
            LOG.exception('Unexpected error handling message.')
            reject_callback()
