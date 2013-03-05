import logging
from flow.command_runner.messages import CommandLineSubmitMessage
import flow.command_runner.executors.nets

LOG = logging.getLogger(__name__)

class CommandLineServiceInterface(object):
    def __init__(self, broker=None, exchange=None, submit_routing_key=None):
        self.broker = broker
        self.exchange = exchange
        self.submit_routing_key = submit_routing_key

    def submit(self, command_line, net_key=None, response_places=None,
            **executor_options):
        message = CommandLineSubmitMessage(
                command_line=command_line,
                net_key=net_key,
                response_places=response_places,
                executor_options=executor_options)

        self.broker.publish(self.exchange, self.submit_routing_key, message)
