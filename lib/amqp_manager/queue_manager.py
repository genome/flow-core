import json
import logging

from delegate_base import Delegate

LOG = logging.getLogger(__name__)

class QueueManager(Delegate):
    def __init__(self, queue_name, decoder=json.loads,
            bad_data_handler=None, message_handler=None,
            **queue_declare_properties):
        Delegate.__init__(self)
        assert callable(message_handler)

        self.queue_name = queue_name
        self.decoder = decoder

        self.bad_data_handler = bad_data_handler
        self.message_handler = message_handler

        self.qd_properties = queue_declare_properties

    def on_message(self, properties, body, ack_callback, reject_callback):
        # This looks messy, but it's important that nothing falls through the
        # cracks.
        try:
            decoded_message = self.decoder(body)
        except:
            LOG.exception('QueueManager %s failed to decode message failed: %s',
                    self, body)
            try:
                if self.bad_data_handler:
                    assert callable(self.bad_data_handler)
                    return self.bad_data_handler(properties, body,
                            ack_callback, reject_callback)
            except:
                LOG.exception(
                        'QueueManager %s caught exception in bad_data_handler',
                        self)
            finally:
                try:
                    reject_result = reject_callback()
                    LOG.warning('Final reject call for bad data succeeded' +
                            ' in QueueManager %s: %s, %s',
                            self, properties, body)
                    return reject_result
                except:
                    return

        try:
            return self.message_handler(properties, decoded_message,
                    ack_callback, reject_callback)
        except:
            LOG.exception('QueueManager %s rejecting message' +
                    ' due to unhandled exception in message handler', self)
            return reject_callback()


    def on_channel_open(self, channel_manager):
        self._channel_manager = channel_manager
        LOG.debug('Declaring queue %s', self.queue_name)
        channel_manager.queue_declare(self._on_declare_queue_ok,
                self.queue_name, **self.qd_properties)

    def on_channel_closed(self, channel_manager):
        # XXX does this take a method frame instead?
        LOG.debug('QueueManager %s got on_channel_closed for %s',
                self, channel_manager)
        self._channel_manager = None


    def _on_declare_queue_ok(self, method_frame):
        LOG.debug("Queue '%s' successfully declared, method_frame = %s",
                self.queue_name, method_frame)
        LOG.debug("Beginning consumption of messages from queue '%s'",
                self.queue_name)
        self._channel_manager.basic_consume(self.on_message, self.queue_name)
        self.notify_ready()
