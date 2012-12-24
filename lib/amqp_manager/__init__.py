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

# NOTE pika does not do this itself for some reason
logging.getLogger('pika').addHandler(nh)

from amqp_manager.channel_manager import ChannelManager
from amqp_manager.connection_manager import ConnectionManager
from amqp_manager.exchange_manager import ExchangeManager
from amqp_manager.queue_manager import QueueManager
