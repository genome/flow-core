#!/usr/bin/env python

import argparse
import json
import logging
import os
import pika
import subprocess
import sys


from pprint import pprint

def parse_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument('--return_packet', required=True)

    parser.add_argument('--amqp_host', default='linus202')
    parser.add_argument('--amqp_port', type=int, default=5672)
    parser.add_argument('--amqp_vhost', default='workflow')
    parser.add_argument('--amqp_username', required=True)
    parser.add_argument('--amqp_password', required=True)

    parser.add_argument('--amqp_exchange', required=True)
    parser.add_argument('--amqp_exchange_type', default='topic')

    parser.add_argument('--success_routing_key', required=True)
    parser.add_argument('--failure_routing_key', required=True)

    parser.add_argument('command')
    parser.add_argument('arguments', nargs='*')

    return parser.parse_args()


def connect_to_amqp(args):
    credentials = pika.PlainCredentials(args.amqp_username, args.amqp_password)
    properties = pika.ConnectionParameters(args.amqp_host, args.amqp_port,
            args.amqp_vhost, credentials=credentials)
    connection = pika.BlockingConnection(properties)

    channel = connection.channel()
    exchange = channel.exchange_declare(exchange=args.amqp_exchange,
            type=args.amqp_exchange_type, durable=True)

    return channel


if '__main__' == __name__:
    args = parse_arguments()

    complete_command = [args.command]
    complete_command.extend(args.arguments)

    exit_code = subprocess.call(complete_command)

    if 0 == exit_code:
        routing_key = args.success_routing_key
    else:
        routing_key = args.failure_routing_key

    channel = connect_to_amqp(args)

    return_data = {'workitem': json.loads(args.return_packet),
            'grid_job_id': os.getenv('LSB_JOBID'),
            'exit_code': exit_code}
    channel.basic_publish(exchange=args.amqp_exchange, routing_key=routing_key,
            body=json.dumps(return_data))

    sys.exit(exit_code)
