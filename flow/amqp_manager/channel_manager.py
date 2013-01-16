import logging
import pika
from functools import partial

from delegate_base import Delegate

LOG = logging.getLogger(__name__)

class ChannelManager(Delegate):
    _REQUIRED_DELEGATE_METHODS = ['on_channel_open', 'on_channel_closed']

    def __init__(self, prefetch_count=None, **kwargs):
        Delegate.__init__(self, **kwargs)
        self.prefetch_count = prefetch_count
        self._channel = None


    def on_connection_open(self, connection_manager):
        LOG.debug('Connection open, creating channel')
        connection_manager.channel(self._on_channel_open)

    def on_connection_closed(self, method_frame):
        LOG.debug('Got on_connection closed in channel_manager %s: %s',
                self._channel, method_frame)


    def _on_channel_open(self, channel):
        self._setup_channel(channel)
        self._inform_delegates_about_channel()

    def _setup_channel(self, channel):
        self._channel = channel

        LOG.debug('Channel %s created, adding callback', channel)
        channel.add_on_close_callback(self._on_channel_closed)

        if self.prefetch_count:
            LOG.debug('Setting prefetch count for channel %s to %d',
                    channel, self.prefetch_count)
            channel.basic_qos(prefetch_count=self.prefetch_count)

    def _inform_delegates_about_channel(self):
        for delegate in self.delegates:
            try:
                LOG.debug('Delegating on_channel_open to %s', delegate)
                delegate.on_channel_open(self)
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


    def publish(self, success_callback=None, failure_callback=None,
            **basic_publish_properties):
        delivery_tag = self.basic_publish(**basic_publish_properties)
        if delivery_tag > 0:
            if success_callback:
                success_callback()
        else:
            if failure_callback:
                failure_callback()
        return delivery_tag

    def basic_publish(self, exchange_name=None, routing_key=None, message=None,
            persistent=True, **basic_publish_properties):
        properties = None
        if persistent:
            properties = pika.BasicProperties(delivery_mode=2)

        return self._channel.basic_publish(exchange_name, routing_key,
                message, properties=properties, **basic_publish_properties)


    def exchange_declare(self, *args, **kwargs):
        self._channel.exchange_declare(*args, **kwargs)

    def queue_declare(self, *args, **kwargs):
        self._channel.queue_declare(*args, **kwargs)

    def queue_bind(self, *args, **kwargs):
        self._channel.queue_bind(*args, **kwargs)


    def basic_consume(self, on_message_callback, queue_name):
        self._channel.basic_consume(
                partial(self._on_message_callback, on_message_callback),
                queue_name)

    def _on_message_callback(self, on_message_callback,
            channel, basic_deliver, properties, body):
        ack_callback = make_ack_callback(channel, basic_deliver)
        reject_callback = make_reject_callback(channel, basic_deliver)

        try:
            on_message_callback(properties, body, ack_callback, reject_callback)
        except KeyboardInterrupt:
            raise
        except:
            LOG.exception('ChannelManager %s caught unhandled exception' +
                    ' delivering message to %s', self, on_message_callback)
            reject_callback()

def make_ack_callback(channel, basic_deliver):
    return lambda: channel.basic_ack(basic_deliver.delivery_tag)

def make_reject_callback(channel, basic_deliver):
    return lambda: channel.basic_reject(
            basic_deliver.delivery_tag, requeue=False)
