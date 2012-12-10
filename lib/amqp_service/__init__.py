import logging
import logging.handlers

try:
    nh = logging.handlers.NullHandler()
except AttributeError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

    nh = NullHandler()

logging.getLogger('amqp_service').addHandler(nh)


from connection_manager import ConnectionManager
from service import AMQPService

import dispatcher
import responder
