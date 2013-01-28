import logging
from flow.command_runner.messages import CommandLineSubmitMessage

LOG = logging.getLogger(__name__)

class CommandLineClient(object):
    def __init__(self, broker=None,
            submit_routing_key=None,
            success_routing_key=None,
            failure_routing_key=None,
            error_routing_key=None,
            wrapper=[]):
        self.broker                     = broker
        self.submit_routing_key         = submit_routing_key
        self.success_routing_key = success_routing_key
        self.failure_routing_key = failure_routing_key
        self.error_routing_key   = error_routing_key
        self.wrapper                    = wrapper

    def submit(self, command_line, return_identifier=None, **executor_options):
        message = CommandLineSubmitMessage(
                command_line=self.wrapper + command_line,
                return_identifier=return_identifier,
                success_routing_key=self.success_routing_key,
                failure_routing_key=self.failure_routing_key,
                error_routing_key=self.error_routing_key,
                executor_options=executor_options)

        self.broker.publish(self.submit_routing_key, message)
