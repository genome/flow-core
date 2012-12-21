#!/usr/bin/env python

import logging
import os

from amqp_service import ConnectionManager, AMQPService, dispatcher, responder
from amqp_service import log_formatter

PIKA_LOG_LEVEL = logging.INFO
LOG_LEVEL = logging.DEBUG
LOG_FORMAT = ('%(levelname)-23s %(asctime)s %(name)-60s %(funcName)-45s'
              ' %(lineno)5d: %(message)s')
LOG = logging.getLogger()

DEFAULT_ENVIRONMENT = {
    'LSF_SERVERDIR': '/usr/local/lsf/8.0/linux2.6-glibc2.3-x86_64/etc',
    'LSF_LIBDIR': '/usr/local/lsf/8.0/linux2.6-glibc2.3-x86_64/lib',
    'LSF_BINDIR': '/usr/local/lsf/8.0/linux2.6-glibc2.3-x86_64/bin',
    'LSF_ENVDIR': '/usr/local/lsf/conf'
}

if '__main__' == __name__:
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter.ColorFormatter(LOG_FORMAT))
    console_handler.setLevel(LOG_LEVEL)
    LOG.addHandler(console_handler)
    LOG.setLevel(LOG_LEVEL)
    logging.getLogger('pika').setLevel(PIKA_LOG_LEVEL)

    amqp_url = os.getenv('AMQP_URL')
    if not amqp_url:
        amqp_url = 'amqp://guest:guest@linus202:5672/workflow'
    connection_manager = ConnectionManager(amqp_url)

    lsf_dispatcher = dispatcher.LSFDispatcher(
            default_environment=DEFAULT_ENVIRONMENT)
    submit_responder = responder.DispatchResponder(lsf_dispatcher,
            queue='lsf_submit', exchange='workflow',
            alternate_exchange='workflow.alt')

    service = AMQPService(connection_manager, submit_responder)

    try:
        service.run()
    except KeyboardInterrupt:
        service.stop()
