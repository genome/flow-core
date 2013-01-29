import argparse
import logging
from flow.orchestrator.messages import NodeStatusRequestMessage
import time
import uuid

LOG = logging.getLogger()


class NodeStatusRequestor(object):
    def __init__(self, broker=None, polling_interval=5,
            request_routing_key=None, response_routing_key_template=None,
            queue_template=None, responder_exchange=None):

        self.broker = broker
        self.polling_interval = polling_interval
        self.request_routing_key = request_routing_key

        self.listener_id = uuid.uuid4().hex
        self.queue_name = queue_template % self.listener_id
        self.response_routing_key = (response_routing_key_template
                % self.listener_id)
        self.responder_exchange = responder_exchange

    def parse_args(self, args):
        parser = argparse.ArgumentParser()

        parser.add_argument('node_key', help='Query the status of this node')
        parser.add_argument('expected_statuses', nargs='*', default=[],
                help='If specified, block until status matches one of these')

        parser.add_argument('--polling-interval', type=float, default=None,
                help='The interval with which to poll for node status')

        return parser.parse_args(args=args)

    def run(self, *raw_args):
        args = self.parse_args(raw_args)
        if args.polling_interval:
            self.polling_interval = args.polling_interval

        self.broker.connect()
        self.broker.create_bound_temporary_queue(self.responder_exchange,
                self.response_routing_key, self.queue_name)

        if args.expected_statuses:
            result = self.block_until_status(args.node_key,
                    args.expected_statuses)
        result = self.get_status(args.node_key)

        print result

        return 0

    def block_until_status(self, node_key, done_statuses):
        status = None
        while status not in done_statuses:
            status = self.get_status(node_key)
            LOG.debug('Got status (%s) for node (%s)', status, node_key)
            time.sleep(self.polling_interval)

        return status

    def get_status(self, node_key):
        message = NodeStatusRequestMessage(node_key=node_key,
                response_routing_key=self.response_routing_key)

        self.broker.publish(self.request_routing_key, message)
        response = self.broker.get(self.queue_name)

        return response.status
