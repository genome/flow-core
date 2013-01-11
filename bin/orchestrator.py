#!/usr/bin/env python

import logging

import redis
import flow.brokers.amqp
from flow.orchestrator.handlers import MethodDescriptorHandler, ExecuteNodeHandler
from flow_command_runner.client import CommandLineClient

from flow import configuration

LOG = logging.getLogger()

EXECUTE_WRAPPER = []
SHORTCUT_WRAPPER = []
CALLBACK_QUEUES = {
        'on_success': 'workflow_success',
        'on_failure': 'workflow_failure',
}

if '__main__' == __name__:
    args = configuration.parse_arguments()
    configuration.setup_logging(args.logging_configuration)

    broker = flow.brokers.amqp.AmqpBroker()

    shortcut_service = CommandLineClient(broker,
            submit_routing_key='genome.shortcut.submit',
            success_routing_key='genome.shortcut.success',
            failure_routing_key='genome.shortcut.failure',
            error_routing_key='genome.shortcut.error',
            wrapper=SHORTCUT_WRAPPER)

    execute_service = CommandLineClient(broker,
            submit_routing_key='genome.execute.submit',
            success_routing_key='genome.execute.submit.success',
            failure_routing_key='genome.execute.submit.failure',
            error_routing_key='genome.execute.submit.error',
            wrapper=EXECUTE_WRAPPER)

    services = {
            'genome_shortcut': shortcut_service,
            'genome_execute': execute_service,
    }

    redis_connection = redis.StrictRedis(host='linus129')
    for callback_name, queue_name in CALLBACK_QUEUES.iteritems():
        handler = MethodDescriptorHandler(redis=redis_connection,
                services=services, callback_name=callback_name)
        broker.register_handler(queue_name, handler)

    execute_handler = ExecuteNodeHandler(
            redis=redis_connection, services=services)
    broker.register_handler('flow_execute_node', execute_handler)

    broker.listen()
