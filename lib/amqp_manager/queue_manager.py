import json
import logging

LOG = logging.getLogger(__name__)

class QueueManager(object):
    def __init__(self, queue_name, decoder=json.loads, durable=True,
            bad_data_handler=None, message_handler=None):
        assert callable(message_handler)

        self.queue_name = queue_name
        self.decoder = decoder
        self.durable = durable

        self.bad_data_handler = bad_data_handler
        self.message_handler = message_handler

        self.queue = None
        self._channel = None

    def on_message(self, channel, basic_deliver, properties, body):
        ack_callback = make_ack_callback(channel, basic_deliver)
        reject_callback = make_reject_callback(channel, basic_deliver)

        try:
            decoded_message = self.decoder(body)
        except:
            LOG.exception('QueueManager %s failed to decode message failed: %s',
                    self, body)
            try:
                if self.bad_data_handler:
                    assert callable(self.bad_data_handler)
                    return self.bad_data_handler(body,
                            ack_callback, reject_callback)
            except:
                LOG.exception(
                        'QueueManager %s caught exception in bad_data_handler',
                        self)
            finally:
                # XXX Make sure that the bdh rejected or ack'd
                    # Do multiple ack/rejects work?
                return reject_callback()

        try:
            return self.message_handler(decoded_message,
                    ack_callback, reject_callback)
        except:
            LOG.exception('QueueManager %s rejecting message' +
                    ' due to unhandled exception in message handler', self)
            return reject_callback()


    def on_channel_open(self, channel_manager, channel):
        LOG.debug('Declaring queue %s', self.queue_name)
        self._channel = channel
        channel.queue_declare(self._on_declare_queue_ok,
                self.queue_name, durable=self.durable)

    def on_channel_closed(self, channel):
        LOG.debug('QueueManager %s got on_channel_closed for %s',
                self, channel)
        self.queue = None
        self._channel = None


    def _on_declare_queue_ok(self, queue, method_frame):
        LOG.debug("Queue '%s' successfully declared, method_frame = %s",
                self.queue_name, method_frame)
        self.queue = queue
        LOG.debug("Beginning consumption of messages from queue '%s'",
                self.queue_name)
        self._channel.basic_consume(self.on_message, queue)


def make_ack_callback(channel, basic_deliver):
    return lambda: channel.basic_ack(basic_deliver.delivery_tag)

def make_reject_callback(channel, basic_deliver):
    return lambda: channel.basic_reject(
            basic_deliver.delivery_tag, requeue=False)
