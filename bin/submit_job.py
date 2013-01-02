#!/usr/bin/env python

from flow.amqp_service import log_formatter
import argparse
import json
import logging
import os
import pika
import uuid

DEFAULT_AMQP_URL = 'amqp://guest:guest@localhost:5672/workflow'

DEFAULT_LOG_LEVEL = logging.WARNING
LOG_FORMAT = ('%(levelname)-23s %(asctime)s %(name)-60s %(funcName)-45s'
              ' %(lineno)5d: %(message)s')
LOG = logging.getLogger()


def parse():
    parser = argparse.ArgumentParser()

    parser.add_argument('routing_key',
            help='What routing key to publish the message to')
    parser.add_argument('command_line', nargs='+',
            help='The command to run')

    parser.add_argument('--log_level', type=int, default=DEFAULT_LOG_LEVEL)
    amqp_url_help = '%s\n%s\nDefaults to %s' % (
            'The location of the rabbitmq server.',
            'Overrides AMQP_URL environment variable.',
             DEFAULT_AMQP_URL)
    parser.add_argument('--amqp_url',
            default=os.getenv('AMQP_URL', DEFAULT_AMQP_URL),
            help=amqp_url_help)

    parser.add_argument('--exchange', '-x', default='',
            help='Which exchange to send the job request to')

    parser.add_argument('--exchange_type', '-t', default='direct',
            help='Which type of exchange to send the job request to')

    parser.add_argument('--success_routing_key', '-s', default='fake.success')
    parser.add_argument('--failure_routing_key', '-f', default='fake.failure')
    parser.add_argument('--error_routing_key', '-e', default='fake.error')

    default_return_identifier = uuid.uuid4().hex
    parser.add_argument('--return_identifier',
            default=default_return_identifier)

    default_dir = os.getenv('HOME', '/tmp/service')
    parser.add_argument('--stdout',
            default=os.path.join(default_dir, 'out.log'))
    parser.add_argument('--stderr',
            default=os.path.join(default_dir, 'err.log'))
    parser.add_argument('--working_directory', '-d')

    return parser.parse_args()


if __name__ == '__main__':
    args = parse()

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter.ColorFormatter(LOG_FORMAT))
    console_handler.setLevel(args.log_level)
    LOG.addHandler(console_handler)
    LOG.setLevel(args.log_level)

    data = {
            'command_line': args.command_line,
            'return_identifier': args.return_identifier,
            'success_routing_key': args.success_routing_key,
            'failure_routing_key': args.failure_routing_key,
            'error_routing_key': args.error_routing_key,
            'stdout': args.stdout,
            'stderr': args.stderr,
            }
    if args.working_directory:
        LOG.debug('setting working_directory: %s', args.working_directory)
        data['working_directory'] = args.working_directory

    message = json.dumps(data)
    LOG.debug('message assembled: %s', message)

    LOG.debug('attempting to connect to %s', args.amqp_url)
    conn = pika.BlockingConnection(pika.URLParameters(args.amqp_url))
    chan = conn.channel()
    chan.basic_publish(exchange=args.exchange,
            routing_key=args.routing_key, body=message)
    print args.return_identifier
