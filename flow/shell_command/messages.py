from flow.protocol.message import Message
from flow.protocol import exceptions


class ShellCommandSubmitMessage(Message):
    required_fields = {
            'command_line': list,
            'net_key': basestring,
            'response_places': dict,
            'color': (int, long),
            'color_group_idx': (int, long),
    }

    optional_fields = {
            'inputs_hash_key': basestring,
            'with_outputs': bool,
            'executor_options': dict,
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
