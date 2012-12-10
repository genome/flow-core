import logging
import logging.handlers

try:
    nh = logging.handlers.NullHandler()
except AttributeError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

    nh = NullHandler()

logging.getLogger().addHandler(nh)


from amqp_manager import AMQPManager
from service import AMQPService

import dispatcher
import responder
