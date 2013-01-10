#!/usr/bin/env python

import logging

import redis
import flow.brokers.amqp
from flow.orchestrator.handlers import MethodDescriptorHandler
from flow_command_runner.client import CommandLineClient

from flow import configuration

LOG = logging.getLogger()

EXECUTE_WRAPPER = []
SHORTCUT_WRAPPER = []
CALLBACK_QUEUES = {
        'on_shortcut_success': 'workflow_shortcut_success',
        'on_shortcut_failure': 'workflow_shortcut_failure',
        'on_execute_success': 'workflow_execute_success',
        'on_execute_failure': 'workflow_execute_failure',

        # Maybe?
        'on_execute_job_id': 'workflow_execute_job_id',
}

if '__main__' == __name__:
    args = configuration.parse_arguments()
    configuration.setup_logging(args.logging_configuration)

    broker = flow.brokers.amqp.AmqpBroker()

    shortcut_service = CommandLineClient(broker,
            submit_routing_key='genome.shortcut.submit',
            submit_success_routing_key='genome.shortcut.success',
            submit_failure_routing_key='genome.shortcut.failure',
            submit_error_routing_key='genome.shortcut.error',
            wrapper=SHORTCUT_WRAPPER)

    execute_service = CommandLineClient(broker,
            submit_routing_key='genome.execute.submit',
            submit_success_routing_key='genome.execute.success',
            submit_failure_routing_key='genome.execute.failure',
            submit_error_routing_key='genome.execute.error',
            wrapper=EXECUTE_WRAPPER)

    services = {
            'shortcut': shortcut_service,
            'execute': execute_service,
    }

    redis_connection = redis.StrictRedis(host='vmpool84')
    for callback_name, queue_name in CALLBACK_QUEUES.iteritems():
        handler = MethodDescriptorHandler(redis=redis_connection,
                services=services, callback_name=callback_name)
        broker.register_handler(queue_name, handler)

    broker.listen()
