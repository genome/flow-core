#!/usr/bin/env python

import argparse
import logging
import sys

import flow.brokers.amqp

from flow.orchestrator.handlers import NodeStatusResponseHandler

from flow import configuration

LOG = logging.getLogger()


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--logging_configuration',
            default='config/console_color.yaml')

    parser.add_argument('--polling_interval', type=float, default=5.0)

    parser.add_argument('node_key')

    return parser.parse_args()

if '__main__' == __name__:
    args = parse_arguments()
    configuration.setup_logging(args.logging_configuration)

    broker = flow.brokers.amqp.AmqpBroker()

    handler = NodeStatusResponseHandler(broker,
            node_key=args.node_key, polling_interval=args.polling_interval,
            request_routing_key='flow.status.request',
            response_routing_key='temp.status.response')
    broker.register_handler('process_status_response', handler)

    handler.send_request()
    exit_code = broker.listen()

    sys.exit(exit_code)
