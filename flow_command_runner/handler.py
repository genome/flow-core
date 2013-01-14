import logging
from flow_command_runner.messages import CommandLineResponseMessage

LOG = logging.getLogger(__name__)

class CommandLineSubmitMessageHandler(object):
    def __init__(self, executor=None, broker=None):
        self.executor = executor
        self.broker = broker

    def __call__(self, message):
        LOG.debug('CommandLineSubmitMessageHandler got message')
        executor_options = getattr(message, 'executor_options', {})

        try:
            success, executor_result = self.executor(message.command_line,
                    **executor_options)

            if success:
                response_routing_key = message.success_routing_key
                response_message = CommandLineResponseMessage(
                        job_id=str(executor_result),
                        return_identifier=message.return_identifier)

            else:
                response_routing_key = message.failure_routing_key
                response_message = CommandLineResponseMessage(
                        return_identifier=message.return_identifier)

        # XXX Might need to tweak what exceptions we need to catch
        except RuntimeError as e:
            response_routing_key = message.error_routing_key
            response_message = CommandLineResponseMessage(
                    return_identifier=message.return_identifier,
                    error_message=str(e))

        self.broker.publish(response_routing_key, response_message)
