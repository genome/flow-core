import logging
import logging.handlers

try:
    nh = logging.handlers.NullHandler()
except AttributeError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

    nh = NullHandler()

logging.getLogger('amqp_manager').addHandler(nh)
logging.getLogger('amqp_manager').setLevel(logging.INFO)

# NOTE pika does not do this itself for some reason
logging.getLogger('pika').addHandler(nh)
logging.getLogger('pika').setLevel(logging.INFO)

from amqp_manager.channel_manager import ChannelManager
from amqp_manager.connection_manager import ConnectionManager
from amqp_manager.exchange_manager import ExchangeManager
from amqp_manager.queue_manager import QueueManager
