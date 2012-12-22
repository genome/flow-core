import logging

from pika.spec import Basic

from amqp_manager.channel_manager import ChannelManager

LOG = logging.getLogger(__name__)

class ConfirmingChannelManager(ChannelManager):
    def __init__(self, max_publish_attempts=10, **kwargs):
        self.max_publish_attempts = max_publish_attempts
        self._unconfirmed_messages = {}

        ChannelManager.__init__(self, **kwargs)

    def publish(self, attempts=0, **basic_publish_properties):
        if attempts >= self.max_publish_attempts:
            LOG.warn('Failed to publish message after %d attempts: %s',
                    attempts, basic_publish_properties)
            failure_callback = basic_publish_properties.get('failure_callback')
            if failure_callback:
                return failure_callback()
            return None

        delivery_tag = self.basic_publish(**basic_publish_properties)

        basic_publish_properties['attempts'] = attempts + 1
        self._unconfirmed_messages[delivery_tag] = basic_publish_properties

        return delivery_tag


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

        LOG.debug('Enabling publisher confirms for channel %s', channel)
        channel.confirm_delivery()

        add_confirm_ack_callback(channel, self.on_confirm_ack)
        add_confirm_nack_callback(channel, self.on_confirm_nack)

        self._inform_delegates_about_channel(channel)


def add_confirm_ack_callback(channel, callback):
    channel.callbacks.add(channel.number, Basic.Ack, callback, one_shot=False)

def add_confirm_nack_callback(channel, callback):
    channel.callbacks.add(channel.number, Basic.Nack, callback, one_shot=False)
