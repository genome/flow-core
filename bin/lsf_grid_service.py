#!/usr/bin/env python

import logging

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

    connection_manager = ConnectionManager('amqp://guest:guest@localhost:5672/workflow')

    lsf_dispatcher = dispatcher.LSFDispatcher()
    submit_responder = responder.GridSubmitResponder(
            lsf_dispatcher,
            queue='lsf_submit_job_requests', exchange='lsf',
            succeeded_routing_key='submit.respond')

    service = AMQPService(connection_manager, submit_responder)

    try:
        service.run()
    except KeyboardInterrupt:
        service.stop()
