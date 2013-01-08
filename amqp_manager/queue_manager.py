import logging

from delegate_base import Delegate

LOG = logging.getLogger(__name__)

class QueueManager(Delegate):
    def __init__(self, queue_name, message_handler=None,
            **queue_declare_properties):
        Delegate.__init__(self)
        assert callable(message_handler)

        self.queue_name = queue_name

        self.message_handler = message_handler

        self.qd_properties = queue_declare_properties

    def on_message(self, properties, body, ack_callback, reject_callback):
        try:
            return self.message_handler(properties, body,
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
