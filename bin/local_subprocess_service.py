#!/usr/bin/env python

import logging
import os

from amqp_service import ConnectionManager, AMQPService, dispatcher, responder

LOG_LEVEL = logging.DEBUG
LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOG = logging.getLogger()

if '__main__' == __name__:
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    console_handler.setLevel(LOG_LEVEL)
    LOG.addHandler(console_handler)
    LOG.setLevel(LOG_LEVEL)

    amqp_url = os.getenv('AMQP_URL')
    if not amqp_url:
        amqp_url = 'amqp://guest:guest@linus202:5672/workflow'
    connection_manager = ConnectionManager(amqp_url)

    subprocess_dispatcher = dispatcher.SubprocessDispatcher()
    submit_responder = responder.DispatchResponder(
            subprocess_dispatcher,
            queue='subprocess_submit_job_requests', exchange='subprocess',
            succeeded_routing_key='submit.respond')

    service = AMQPService(connection_manager, submit_responder)

    try:
        service.run()
    except KeyboardInterrupt:
        service.stop()
