#!/usr/bin/env python

import logging
import os

from flow_command_runner.executors import local
from flow_command_runner.handler import CommandLineSubmitMessageHandler

import flow.brokers.amqp

from flow import configuration
import amqp_manager

LOG = logging.getLogger()

if '__main__' == __name__:

    args = configuration.parse_arguments()
    configuration.setup_logging(args.logging_configuration)

    amqp_url = os.getenv('AMQP_URL')
    if not amqp_url:
        amqp_url = 'amqp://guest:guest@localhost:5672/%2F'
        LOG.warning("No AMQP_URL found, using '%s' by default", amqp_url)

    arguments = {'alternate-exchange': 'workflow.alt'}
    exchange_manager = amqp_manager.ExchangeManager('workflow',
            durable=True, persistent=True, **arguments)
    broker = flow.brokers.amqp.AmqpBroker(exchange_manager=exchange_manager)

    executor = local.SubprocessExecutor()
    handler = CommandLineSubmitMessageHandler(executor=executor)

    listener = flow.brokers.amqp.AmqpListener(
            delivery_callback=handler.message_handler, broker=broker)

    queue_manager = amqp_manager.QueueManager('subprocess_submit',
            message_handler=listener.on_message, durable=True)

    channel_manager = amqp_manager.ChannelManager(
            delegates=[exchange_manager, queue_manager])
    connection_manager = amqp_manager.ConnectionManager(
            amqp_url,
            delegates=[channel_manager])

    connection_manager.start()
