import logging
from flow_command_runner.messages import CommandLineResponseMessage

LOG = logging.getLogger(__name__)

class CommandLineMessageHandler(object):
    def __init__(self, executor=None, **kwargs):
        self.RespondingHandler.__init__(self, **kwargs)
        self.executor = executor

    def message_handler(self, message, broker):
        executor_options = getattr(message, 'executor_options', {})
        try:
            success, result = self.executor.launch_job(message.command_line,
                    **executor_options)

            if success:
                routing_key = message.success_routing_key
                message = CommandLineResponseMessage(
                        status='success', job_id=executor_result,
                        return_identifier=message.return_identifier)
            else:
                routing_key = message.failure_routing_key
                message = CommandLineResponseMessage(status='failure',
                        return_identifier=message.return_identifier)

        # XXX Might need to tweak what exceptions we need to catch
        except RuntimeError as e:
            routing = message.error_routing_key
            message = CommandLineResponseMessage(status='error',
                    return_identifier=message.return_identifier,
                    error_message=str(e))

        broker.publish(response_routing_key, response_message)
