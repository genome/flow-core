from flow.protocol.message import Message
from flow.protocol import exceptions


class ShellCommandSubmitMessage(Message):
    required_fields = {
            'command_line': list,
            'net_key': basestring,
            'response_places': dict,
    }

    optional_fields = {
            'token_color': int,
            'executor_options': dict,

            # The following options are deprecated
            'success_routing_key': basestring,
            'failure_routing_key': basestring,
            'error_routing_key': basestring,
            'return_identifier': object,
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
