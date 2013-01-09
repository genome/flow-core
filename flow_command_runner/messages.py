from flow.protocol.message import Message
from flow.protocol import exceptions


class CommandLineSubmitMessage(Message):
    required_fields = {
            'return_identifier': object,
            'command_line': list,

            'success_routing_key': basestring,
            'failure_routing_key': basestring,
            'error_routing_key': basestring,
    }

    optional_fields = {
            'executor_options': dict,
            'status': basestring,
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
            'return_identifier': object,
            'status': basestring,
    }
    optional_fields = {
            'job_id': basestring,
            'error_message': basestring,
    }
