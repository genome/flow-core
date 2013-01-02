from flow.protocol.message import Message
from flow.protocol import exceptions


class SubmitCommandLineMessage(Message):
    required_fields = {
            'node_id': str,
            'step_id': str,
            'command_line': list,
            'success_routing_key': str,
            'failure_routing_key': str,
            'error_routing_key': str,
            'status': str,
    }

    optional_fields = {'dispatcher_options': dict}

    def validate(self):
        try:
            self.command_line = map(str, self.command_line)
        except TypeError:
            raise exceptions.InvalidMessageException(
                    'Invalid type in command_line.')

        dispatcher_options = getattr(self, 'dispatcher_options', {})
        for k in dispatcher_options.iterkeys():
            type_ = type(k)
            if type_ != str and type_ != unicode:
                raise exceptions.InvalidMessageException(
                        'Invalid type (%s) as dispatcher_options key.', type_)
