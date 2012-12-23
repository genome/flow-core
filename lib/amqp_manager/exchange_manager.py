import json
import logging

LOG = logging.getLogger(__name__)

class ExchangeManager(object):
    def __init__(self, exchange_name, encoder=json.dumps,
            exchange_type='topic', durable=True, **exchange_declare_arguments):
        self.exchange_name = exchange_name
        self.exchange_type = exchange_type
        self.durable = durable

        self.encoder = encoder
        self._ed_arguments = exchange_declare_arguments

    def publish(self, routing_key, unencoded_message,
            **basic_publish_properties):
        encoded_message = self.encoder(unencoded_message)

        self._channel_manager.publish(exchange_name=self.exchange_name,
                routing_key=routing_key, message=encoded_message,
                **basic_publish_properties)

    def on_channel_open(self, channel_manager, channel):
        LOG.debug('Declaring %s exchange %s on channel %s',
                self.exchange_type, self.exchange_name, channel)
        self._channel_manager = channel_manager
        channel.exchange_declare(self._on_exchange_declare_ok,
                self.exchange_name, exchange_type=self.exchange_type,
                durable=self.durable, arguments=self._ed_arguments)

    def on_channel_closed(self, channel):
        LOG.debug('Got on_channel_closed in exchange_manager for %s',
                self.exchange_name)
        self._channel_manager = None

    def _on_exchange_declare_ok(self, method_frame):
        LOG.debug('Exchange declare OK for exchange %s', self.exchange_name)
