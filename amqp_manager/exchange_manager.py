import logging

from delegate_base import Delegate

LOG = logging.getLogger(__name__)

class ExchangeManager(Delegate):
    def __init__(self, exchange_name, exchange_type='topic', durable=True,
            persistent=None, basic_publish_properties={},
            **exchange_declare_arguments):
        Delegate.__init__(self)
        self.exchange_name = exchange_name
        self.exchange_type = exchange_type
        self.durable = durable
        self.persistent = persistent

        self._ed_arguments = exchange_declare_arguments
        self._bp_properties = basic_publish_properties

    def publish(self, routing_key=None, message=None):
        self._channel_manager.publish(exchange_name=self.exchange_name,
                routing_key=routing_key, message=message,
                persistent=self.persistent, **self._bp_properties)

    def on_channel_open(self, channel_manager):
        self._channel_manager = channel_manager
        if self.exchange_name:
            LOG.debug('Declaring %s exchange %s on channel %s',
                    self.exchange_type, self.exchange_name, channel_manager)
            channel_manager.exchange_declare(self._on_exchange_declare_ok,
                    self.exchange_name, exchange_type=self.exchange_type,
                    durable=self.durable, arguments=self._ed_arguments)
        else:
            LOG.debug('Empty or missing exchange name, not declaring.')
            self.notify_ready()

    def on_channel_closed(self, channel):
        LOG.debug('Got on_channel_closed in exchange_manager for %s',
                self.exchange_name)
        self._channel_manager = None

    def _on_exchange_declare_ok(self, method_frame):
        LOG.debug('Exchange declare OK for exchange %s', self.exchange_name)
        self.notify_ready()
