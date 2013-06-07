from flow.protocol.message import Message
from flow.protocol import exceptions


class ShellCommandSubmitMessage(Message):
    required_fields = {
            'command_line': list,
            'group_id': (int, long),
            'user_id': (int, long),
    }

    optional_fields = {
            'callback_data': dict,
            'environment': dict,
            'executor_data': dict,
            'resource_limit': dict,
            'resource_request': dict,
            'resource_reserve': dict,
            'working_directory': basestring,
    }

    def validate(self):
        if not self.command_line:
            raise exceptions.InvalidMessageException('Empty command_line.')
        for word in self.command_line:
            if not isinstance(word, basestring):
                raise exceptions.InvalidMessageException(
                        'Invalid type in command_line: %s' % word)

        environment = getattr(self, 'environment', {})
        for k, v in environment.iteritems():
            if not isinstance(k, basestring):
                raise exceptions.InvalidMessageException(
                        'Invalid type for environment key: "%s"' % k)

            if not isinstance(v, basestring):
                raise exceptions.InvalidMessageException(
                        'Invalid type for environment value (key = %s): "%s"'
                        % (k, v))
