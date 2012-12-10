#!/usr/bin/env python

import logging

from amqp_service import ConnectionManager, AMQPService, dispatcher, responder

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOG = logging.getLogger(__name__)

if '__main__' == __name__:
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

    connection_manager = ConnectionManager('amqp://guest:guest@localhost:5672/%2fworkflow')

    lsf_dispatcher = dispatcher.LSFDispatcher()
    submit_responder = responder.GridSubmitResponder(
            lsf_dispatcher,
            queue='lsf_submit_job_requests', exchange='grid',
            succeeded_routing_key='grid.submit.nofitication.success')

    service = AMQPService(connection_manager, submit_responder)

    try:
        service.run()
    except KeyboardInterrupt:
        service.stop()
