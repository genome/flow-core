import logging
from flow_command_runner.messages import CommandLineSubmitMessage

LOG = logging.getLogger(__name__)

class CommandLineClient(object):
    def __init__(self, broker,
            submit_routing_key=None,
            submit_success_routing_key=None,
            submit_failure_routing_key=None,
            submit_error_routing_key=None,
            on_submit_success_callback=None,
            on_submit_failure_callback=None,
            on_submit_error_callback=None,
            on_complete_success_callback=None,
            on_complete_failure_callback=None,
            on_complete_error_callback=None,
            wrapper=[]):
        self.broker                     = broker
        self.submit_routing_key         = submit_routing_key
        self.submit_success_routing_key = submit_success_routing_key
        self.submit_failure_routing_key = submit_failure_routing_key
        self.submit_error_routing_key   = submit_error_routing_key
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

    def on_submit_success(self, message):
        job_id = getattr(message, 'job_id', None)
        self.on_submit_success_callback(message.return_identifier, job_id)

    def on_submit_failure(self, message):
        self.on_submit_failure_callback(message.return_identifier)

    def on_submit_error(self, message):
        error_message = getattr(message, 'error_message', '')
        self.on_submit_error_callback(message.return_identifier, error_message)

    def on_complete_success_callback(self, message):
        self.on_complete_success_callback(message.return_identifier)

    def on_complete_failure_callback(self, message):
        self.on_complete_failure_callback(message.return_identifier)

    def on_complete_error_callback(self, message):
        self.on_complete_error_callback(message.return_identifier)
