import logging

from pika.spec import Basic

LOG = logging.getLogger(__name__)

class ChannelManager(object):
    def __init__(self, prefetch_count=None, delegates=[]):
        self.prefetch_count = prefetch_count
        self.delegates = delegates

        self._channel = None

    def on_connection_open(self, connection):
        LOG.debug('Connection open, creating channel')
        connection.channel(self._on_channel_open)

    def on_connection_closed(self, method_frame):
        LOG.debug('Got on_connection closed in channel_manager %s: %s',
                self._channel, method_frame)


    def _on_channel_open(self, channel):
        self._setup_channel(channel)
        self._inform_delegates_about_channel(channel)

    def _setup_channel(self, channel):
        self._channel = channel

        LOG.debug('Channel %s created, adding callback', channel)
        channel.add_on_close_callback(self._on_channel_closed)

        if self.prefetch_count:
            LOG.debug('Setting prefetch count for channel %s to %d',
                    channel, self.prefetch_count)
            channel.basic_qos(prefetch_count=self.prefetch_count)

    def _inform_delegates_about_channel(self, channel):
        for delegate in self.delegates:
            try:
                delegate.on_channel_open(channel)
            except:
                LOG.exception('Delegating on_channel_open to %s failed',
                        delegate)


    def _on_channel_closed(self, channel):
        LOG.debug('Channel %s closed', channel)

        for delegate in self.delegates:
            try:
                delegate.on_channel_closed(channel)
            except:
                LOG.exception('Delegating on_channel_closed to %s failed',
                        delegate)

        self._channel = None

    def basic_publish(success_callback=None, failure_callback=None,
            **basic_publish_properties):
        self._raw_publish(**basic_publish_properties)
        if success_callback:
            success_callback()


    def _raw_publish(exchange_name=None, routing_key=None, message=None,
            persistent=True, **basic_publish_properties):
        properties = None
        if persistent:
            properties = pika.BasicProperties(delivery_mode=2)

        return self.channel.basic_publish(exchange_name, routing_key,
                message, properties=properties, **basic_publish_properties)


class ConfirmingChannelManager(ChannelManager):
    def __init__(self, max_publish_attempts=10, **kwargs):
        self.max_publish_attempts = max_publish_attempts
        self._unconfirmed_messages = {}

        ChannelManager.__init__(self, **kwargs)

    def basic_publish(attempts=0, **basic_publish_properties):
        if attempts >= self.max_publish_attempts:
            LOG.warn('Failed to publish message after %d attempts: %s',
                    attempts, basic_publish_properties)
            failure_callback = basic_publish_properties.get('failure_callback')
            if failure_callback:
                failure_callback()

        delivery_tag = ChannelManager._raw_publish(**basic_publish_properties)

        basic_publish_properties['attempts'] = attempts + 1
        self._unconfirmed_messages[delivery_tag] = basic_publish_properties)


    def on_confirm_ack(self, method_frame):
        delivery_tag = method_frame.method.delivery_tag
        basic_publish_properties = self._unconfirmed_messages.pop(delivery_tag)
        success_callback = basic_publish_properties.get('success_callback')
        if success_callback:
            success_callback()

    def on_confirm_nack(self, method_frame):
        delivery_tag = method_frame.method.delivery_tag
        basic_publish_properties = self._unconfirmed_messages.pop(delivery_tag)
        self.basic_publish(**basic_publish_properties)


    def _on_channel_open(self, channel):
        self._setup_channel(channel)

        if self.publisher_confirms:
            LOG.debug('Enabling publisher confirms for channel %s', channel)
            channel.confirm_delivery()

            add_confirm_ack_callback(channel, self.on_confirm_ack)
            add_confirm_nack_callback(channel, self.on_confirm_nack)

        self._inform_delegates_about_channel(channel)


def add_confirm_ack_callback(channel, callback):
    channel.callbacks.add(channel.number, Basic.Ack, callback, one_shot=False)

def add_confirm_nack_callback(channel, callback):
    channel.callbacks.add(channel.number, Basic.Nack, callback, one_shot=False)
