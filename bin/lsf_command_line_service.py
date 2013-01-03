#!/usr/bin/env python

import logging
import os

from flow_command_runner.executors import lsf
from flow_command_runner import service

from flow import configuration
import amqp_manager


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

    amqp_url = os.getenv('AMQP_URL')
    if not amqp_url:
        amqp_url = 'amqp://guest:guest@localhost:5672/%2F'
        LOG.warning("No AMQP_URL found, using '%s' by default", amqp_url)

    arguments = {'alternate-exchange': 'workflow.alt'}
    exchange_manager = amqp_manager.ExchangeManager('workflow',
            durable=True, **arguments)
    lsf_dispatcher = lsf.LSFExecutor(
            default_environment=DEFAULT_ENVIRONMENT)
    command_service = service.CommandLineService(lsf_dispatcher,
            exchange_manager, persistent=True)

    queue_manager = amqp_manager.QueueManager('lsf_submit',
            bad_data_handler=command_service.bad_data_handler,
            message_handler=command_service.message_handler,
            durable=True)

    channel_manager = amqp_manager.ChannelManager(
            delegates=[exchange_manager, queue_manager])
    connection_manager = amqp_manager.ConnectionManager(
            amqp_url,
            delegates=[channel_manager])

    connection_manager.start()
