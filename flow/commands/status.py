import argparse
from flow.commands.base import CommandBase
import logging
from flow.orchestrator.messages import NodeStatusRequestMessage
import time
import uuid

LOG = logging.getLogger()


class StatusCommand(CommandBase):
    default_logging_mode = 'silent'

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


    @staticmethod
    def annotate_parser(parser):
        parser.add_argument('node_key', help='Query the status of this node')
        parser.add_argument('--expected-status', nargs='*', default=[],
                help='If specified, block until status matches one of these')

        parser.add_argument('--polling-interval', type=float, default=None,
                help='The interval with which to poll for node status')



    def __call__(self, parsed_arguments):
        if parsed_arguments.polling_interval:
            self.polling_interval = parsed_arguments.polling_interval

        self.broker.connect()
        try:
            self.create_queue()

            if parsed_arguments.expected_status:
                result = self.block_until_status(parsed_arguments.node_key,
                        parsed_arguments.expected_status)
            result = self.get_status(parsed_arguments.node_key)

            print result
        except KeyboardInterrupt:
            self.broker.disconnect()

        return 0

    def create_queue(self):
            self.broker.create_bound_temporary_queue(self.responder_exchange,
                    self.response_routing_key, self.queue_name)

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
