import logging
import pika

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
