#!/usr/bin/env python

import logging
import os

from amqp_service.dispatcher import subprocess_dispatcher
from amqp_service import dispatch_service, log_formatter
import amqp_manager

PIKA_LOG_LEVEL = logging.INFO
LOG_LEVEL = logging.DEBUG
LOG_FORMAT = ('%(levelname)-23s %(asctime)s %(name)-60s %(funcName)-45s'
              ' %(lineno)5d: %(message)s')
LOG = logging.getLogger()

if '__main__' == __name__:
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter.ColorFormatter(LOG_FORMAT))
    console_handler.setLevel(LOG_LEVEL)
    LOG.addHandler(console_handler)
    LOG.setLevel(LOG_LEVEL)
    logging.getLogger('pika').setLevel(PIKA_LOG_LEVEL)

    amqp_url = os.getenv('AMQP_URL')
    if not amqp_url:
        amqp_url = 'amqp://guest:guest@localhost:5672/%2F'
        LOG.warning("No AMQP_URL found, using '%s' by default", amqp_url)

    arguments = {'alternate-exchange': 'workflow.alt'}
    exchange_manager = amqp_manager.ExchangeManager('workflow',
            durable=True, **arguments)
    subprocess_dispatcher = subprocess_dispatcher.SubprocessDispatcher()
    service = dispatch_service.DispatchService(subprocess_dispatcher,
            exchange_manager, persistent=True)

    queue_manager = amqp_manager.QueueManager('subprocess_submit',
            bad_data_handler=service.bad_data_handler,
            message_handler=service.message_handler,
            durable=True)

    channel_manager = amqp_manager.ChannelManager(
            delegates=[exchange_manager, queue_manager])
    connection_manager = amqp_manager.ConnectionManager(
            amqp_url,
            delegates=[channel_manager])

    connection_manager.start()
