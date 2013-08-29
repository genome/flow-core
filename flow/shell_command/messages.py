from flow.protocol.message import Message
from flow.protocol import exceptions


ALLOWED_RESOURCE_TYPES = set(['limit', 'request', 'reserve'])


class ShellCommandSubmitMessage(Message):
    required_fields = {
            'command_line': list,
            'group_id': (int, long),
            'umask': (int, long),
            'user_id': (int, long),
    }

    optional_fields = {
            'callback_data': dict,
            'environment': dict,
            'executor_data': dict,
            'groups': list,
            'resources': dict,
            'working_directory': basestring,
    }

    def validate(self):
        self.validate_command_line()
        self.validate_environment()
        self.validate_resources()

    def validate_command_line(self):
        if not self.command_line:
            raise exceptions.InvalidMessageException('Empty command_line.')
        for word in self.command_line:
            if not isinstance(word, basestring):
                raise exceptions.InvalidMessageException(
                        'Invalid type in command_line: %s' % word)

    def validate_environment(self):
        environment = self.get('environment', {})
        for k, v in environment.iteritems():
            if not isinstance(k, basestring):
                raise exceptions.InvalidMessageException(
                        'Invalid type for environment key: "%s"' % k)

            if not isinstance(v, basestring):
                raise exceptions.InvalidMessageException(
                        'Invalid type for environment value (key = %s): "%s"'
                        % (k, v))

    def validate_resources(self):
        resources = self.get('resources', {})
        for k, v in resources.iteritems():
            if k not in ALLOWED_RESOURCE_TYPES:
                raise exceptions.InvalidMessageException(
                        'Invalid type for resource (%s).  '
                        'Allowed values are: %s' % (k, ALLOWED_RESOURCE_TYPES))
            if not isinstance(v, dict):
                raise exceptions.InvalidMessageException(
                        'Invalid value for resource type (%s).  '
                        'Expected dict, but got %s:  %s' % (k, type(v), v))

            for subkey, subval in v.iteritems():
                if not isinstance(subkey, basestring):
                    raise exceptions.InvalidMessageException(
                            'Invalid type for resource name.  '
                            'Expected string, but got %s: %s'
                            % (type(subkey), subkey))
                if not isinstance(subval, (basestring, float, int, long)):
                    raise exceptions.InvalidMessageException(
                            'Expected scalar type for resource value.  '
                            'Got %s instead: %s' % (type(subval), subval))
