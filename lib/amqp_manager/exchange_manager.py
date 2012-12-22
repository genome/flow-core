import json

from pika.spec import Basic

class ExchangeManager(object):

    def __init__(self, exchange_name, encoder=json.dumps, exchange_type='topic',
            durable=True, **exchange_declare_arguments):
        self.exchange_name = exchange_name
        self.exchange_type = exchange_type
        self.durable = durable

        self.encoder = encoder
        self._ed_arguments = exchange_declare_arguments

        self._unconfirmed_messages = {}

    def publish(self, routing_key, unencoded_message,
            persistent=True, confirm_callback=None, **basic_publish_properties):
        if confirm_callback is not None:
            assert callable(confirm_callback)
        encoded_message = self.encoder(unencoded_message)

        self._publish_encoded_message(confirm_callback, routing_key,
                encoded_message, persistent, basic_publish_properties)

    def _publish_encoded_message(callback, routing_key, message, persistent,
            attempts=0, **basic_publish_properties):
        properties = None
        if persistent:
            properties = delivery_mode = 2

        return self.channel.basic_publish(self.exchange_name, self.routing_key,
                message, properties=properties, **basic_publish_properties)

    def on_channel_open(self, channel):
        LOG.debug('Adding publisher confirm callbacks to channel %s', channel)
        self.channel = channel
        channel.exchange_declare(self._on_exchange_declare_ok,
                self.exchange_name, exchange_type=self.exchange_type,
                durable=self.durable, arguments=self._ed_arguments)

    def on_channel_closed(self, channel):
        LOG.debug('Got on_channel_close in exchange_manager for %s',
                self.exchange_name)
        self.channel = None

    def _on_exchange_declare_ok(self, method_frame):
        LOG.debug('Exchange declare OK for exchange %s', self.exchange_name)



class ConfirmingExchangeManager(ExchangeManager):
    def _publish_encoded_message(callback, routing_key, message, persistent,
            attempts=0, **basic_publish_properties):
        delivery_tag = ExchangeManager._publish_encoded_message(callback,
                routing_key, message, persistent, attempts,
                **basic_publish_properties)

        self._unconfirmed_messages[delivery_tag] = (callback, routing_key,
                encoded_message, persistent, attempts, basic_publish_properties)

    def on_confirm_ack(self, method_frame):
        delivery_tag = method_frame.method.delivery_tag
        (callback, routing_key, message,
                persistent, attempts, basic_publish_properties
                ) = self._unconfirmed_messages[delivery_tag]
        del self._unconfirmed_messages[delivery_tag]
        if callback:
            callback()

    def on_confirm_nack(self, method_frame):
        delivery_tag = method_frame.method.delivery_tag
        (callback, routing_key, message,
                persistent, attempts, basic_publish_properties
                ) = self._unconfirmed_messages[delivery_tag]
        del = self._unconfirmed_messages[delivery_tag]
        self._publish_encoded_message(callback,
                routing_key, message, persistent,
                attempts=attempts + 1, **basic_publish_properties)


    def on_channel_open(self, channel):
        ExchangeManager.on_channel_open(self, channel)

        add_confirm_ack_callback(channel, self.on_confirm_ack)
        add_confirm_nack_callback(channel, self.on_confirm_nack)


def add_confirm_ack_callback(channel, callback):
    channel.callbacks.add(channel.number, Basic.Ack, callback, one_shot=False)

def add_confirm_nack_callback(channel, callback):
    channel.callbacks.add(channel.number, Basic.Nack, callback, one_shot=False)
