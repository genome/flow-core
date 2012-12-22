import json
import logging

LOG = logging.getLogger(__name__)

class QueueManager(object):
    def __init__(self, queue_name, decoder=json.loads, durable=True,
            bad_data_handler=None, message_handler=None):
        assert callable(bad_data_handler)
        assert callable(message_handler)

        self.queue_name = queue_name
        self.decoder = decoder
        self.bad_data_handler = bad_data_handler
        self.message_handler = message_handler

    def on_message(self, channel, basic_deliver, properties, body):
        ack_callback = make_ack_callback(channel, basic_deliver)
        reject_callback = make_reject_callback(channel, basic_deliver)

        try:
            decoded_message = self.decoder(body)
        except:
            self.bad_data_handler(body, ack_callback, reject_callback)
            # XXX Make sure that the bdh rejected or ack'd
                # Do multiple ack/rejects work?
            reject_callback()

        try:
            self.message_handler(decoded_message, ack_callback, reject_callback)
        except:
            LOG.exception('Rejecting message due to unhandled exception')
            reject_callback()


    def on_channel_open(self, channel):
        LOG.debug('Declaring queue %s', self.queue_name)
        channel.queue_declare(self._on_declare_queue_ok,
                queue_name, durable=self.durable)

    def on_channel_closed(self, channel):
        LOG.debug('Got on_channel_close in queue_manager for %s',
                self.queue_name)
        self.queue = None


    def _on_declare_queue_ok(self, queue, method_frame):
        LOG.debug("Queue '%s' successfully declared, method_frame = %s",
                self.queue_name, method_fram)
        self.queue = queue
        LOG.debug("Beginning consumption of messages from queue '%s'",
                self.queue_name)
        self.channel.basic_consume(self.on_message, queue)


def make_ack_callback(channel, basic_deliver):
    return lambda: channel.basic_ack(basic_deliver.delivery_tag)

def make_reject_callback(channel, basic_deliver):
    return lambda: channel.basic_reject(
            basic_deliver.delivery_tag, requeue=False)
