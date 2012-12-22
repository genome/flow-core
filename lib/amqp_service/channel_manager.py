import logging

LOG = logging.getLogger(__name__)

class ChannelManager(object):
    def __init__(self, prefetch_count=None,
            publisher_confirms=True, delegates=[]):
        self._channel = None
        self.prefetch_count = prefetch_count
        self.publisher_confirms = publisher_confirms
        self.delegates = delegates


    def on_connection_open(self, connection):
        LOG.debug('Creating channel')
        connection.channel(self._on_channel_open)

    def on_connection_closed(self, method_frame):
        LOG.debug('Channel closed, method_frame = %s', method_frame)


    def _on_channel_open(self, channel):
        self._channel = channel

        LOG.debug('Channel %s created, adding callback', channel)
        channel.add_on_close_callback(self._on_channel_closed)

        if self.prefetch_count:
            LOG.debug('Setting prefetch count for channel %s to %d',
                    channel, self.prefetch_count)
            channel.basic_qos(prefetch_count=self.prefetch_count)

        if self.publisher_confirms:
            LOG.debug('Enabling publisher confirms for channel %s', channel)
            channel.confirm_delivery()

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
