import logging
import pika

LOG = logging.getLogger(__name__)

class AMQPManager(object):
    def __init__(self, url):
        self.url = url

        self._connection = None
        self._subscriptions = {}
        self._channels = {}

    def subscribe(self, queue, exchange, callback):
        self._subscriptions[queue] = callback
        self._connection.channel(
                fon_open_callback=self.create_on_open_channel_callback(
                    queue, exchange, callback))

    def create_on_open_channel_callback(self, queue, exchange, on_message_callback):
        def on_open_channel_callback(channel):
            self._channels[queue] = channel
            channel.exchange_declare(exchange_name=exchange,
                    exchange_type=exchange_type)
            channel.basic_consume(on_message_callback, queue)
        return on_open_channel_callback


    def run(self):
        pass

    def stop(self):
        pass
