#!/usr/bin/env python

import logging

from flow_command_runner.executors import local
from flow_command_runner.handler import CommandLineSubmitMessageHandler

import flow.brokers.amqp

from flow import configuration
import amqp_manager

LOG = logging.getLogger()

if '__main__' == __name__:
    args = configuration.parse_arguments()
    configuration.setup_logging(args.logging_configuration)

    broker = flow.brokers.amqp.AmqpBroker()

    executor = local.SubprocessExecutor()
    handler = CommandLineSubmitMessageHandler(executor=executor, broker=broker)
    broker.register_handler('subprocess_submit', handler)

    broker.listen()
