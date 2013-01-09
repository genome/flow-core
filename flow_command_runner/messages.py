from flow.protocol.message import Message
from flow.protocol import exceptions


class CommandLineSubmitMessage(Message):
    required_fields = {
            'return_identifier': object,
            'command_line': list,

            'success_routing_key': str,
            'failure_routing_key': str,
            'error_routing_key': str,
    }

    optional_fields = {
            'executor_options': dict,
            'status': str,
    }

    def validate(self):
        try:
            self.command_line = map(str, self.command_line)
        except TypeError:
            raise exceptions.InvalidMessageException(
                    'Invalid type in command_line.')

        executor_options = getattr(self, 'executor_options', {})
        for k in executor_options.iterkeys():
            if not isinstance(k, basestring):
                raise exceptions.InvalidMessageException(
                        'Invalid type for executor_options key: %s', k)

class CommandLineResponseMessage(Message):
    required_fields = {
            'return_identifier': str,
            'status': str,
    }
    optional_fields = {
            'job_id': str,
            'error_message': str,
    }
