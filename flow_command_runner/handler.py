import logging
from flow_command_runner.messages import CommandLineResponseMessage

LOG = logging.getLogger(__name__)

class CommandLineSubmitMessageHandler(object):
    def __init__(self, executor=None):
        self.executor = executor

    def message_handler(self, message, broker):
        executor_options = getattr(message, 'executor_options', {})
        try:
            success, executor_result = self.executor(message.command_line,
                    **executor_options)

            if success:
                response_routing_key = message.success_routing_key
                response_message = CommandLineResponseMessage(
                        status='success', job_id=executor_result,
                        return_identifier=message.return_identifier)
            else:
                response_routing_key = message.failure_routing_key
                response_message = CommandLineResponseMessage(status='failure',
                        return_identifier=message.return_identifier)

        # XXX Might need to tweak what exceptions we need to catch
        except RuntimeError as e:
            response_routing_key = message.error_routing_key
            response_message = CommandLineResponseMessage(status='error',
                    return_identifier=message.return_identifier,
                    error_message=str(e))

        broker.publish(response_routing_key, response_message)
