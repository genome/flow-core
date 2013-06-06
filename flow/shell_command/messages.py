from flow.protocol.message import Message
from flow.protocol import exceptions


class ShellCommandSubmitMessage(Message):
    required_fields = {
            'command_line': list,
            'group_id': (int, long),
            'user_id': (int, long),
            'working_directory': basestring,
    }

    optional_fields = {
            'callback_data': dict,
            'environment': dict,
            'executor_data': dict,

    # some callback datas
    #            'net_key': basestring,
    #            'color': (int, long),
    #            'color_group_idx': (int, long),
    # some other datas
        #            'inputs_hash_key': basestring,
        #            'with_outputs': bool,
    }

    def validate(self):
        for word in self.command_line:
            if not isinstance(word, basestring):
                raise exceptions.InvalidMessageException(
                        'Invalid type in command_line: %s' % word)
        else:
            raise exceptions.InvalidMessageException('Empty command_line.')

        environment = getattr(self, 'environment', {})
        for k, v in environment.iteriems():
            if not isinstance(k, basestring):
                raise exceptions.InvalidMessageException(
                        'Invalid type for environment key: "%s"' % k)

            if not isinstance(v, basestring):
                raise exceptions.InvalidMessageException(
                        'Invalid type for environment value (key = %s): "%s"'
                        % (k, v))
