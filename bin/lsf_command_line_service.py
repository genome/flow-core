#!/usr/bin/env python

import logging
import os

from flow_command_runner.executors import lsf
from flow_command_runner.handler import CommandLineSubmitMessageHandler

import flow.brokers.amqp

from flow import configuration


LOG = logging.getLogger()

DEFAULT_ENVIRONMENT = {
    'LSF_SERVERDIR': '/usr/local/lsf/8.0/linux2.6-glibc2.3-x86_64/etc',
    'LSF_LIBDIR': '/usr/local/lsf/8.0/linux2.6-glibc2.3-x86_64/lib',
    'LSF_BINDIR': '/usr/local/lsf/8.0/linux2.6-glibc2.3-x86_64/bin',
    'LSF_ENVDIR': '/usr/local/lsf/conf'
}

if '__main__' == __name__:
    args = configuration.parse_arguments()
    configuration.setup_logging(args.logging_configuration)

    broker = flow.brokers.amqp.AmqpBroker()

    executor = lsf.LSFExecutor(
            default_environment=DEFAULT_ENVIRONMENT)
    handler = CommandLineSubmitMessageHandler(executor=executor, broker=broker)
    broker.register_handler('lsf_submit', handler)

    broker.listen()
