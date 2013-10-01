from flow.protocol.message import Message
from flow.protocol import exceptions


ALLOWED_RESOURCE_TYPES = set(['limit', 'request', 'reserve'])


class ShellCommandSubmitMessage(Message):
    required_fields = {
            'group_id': (int, long),
            'user_id': (int, long),
    }

    optional_fields = {
            'callback_data': dict,
            'environment': dict,
            'executor_data': dict,

            'working_directory': basestring,

            # Deprecated - moved into executor_data
            'command_line': list,
            'resources': dict,
            'umask': (int, long),

            # Deprecated - removed
            'groups': list,
    }

    def validate(self):
        self.validate_command_line()
        self.validate_environment()
        self.validate_resources()
        self.validate_permissions()

    def validate_command_line(self):
        command_line = self.get('executor_data', {}).get('command_line', [])
        if not isinstance(command_line, (tuple, list)):
            raise exceptions.InvalidMessageException(
                    'command_line must be a list or tuple.  Got: %r'
                    % command_line)
        for word in command_line:
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
        resources = self.get('executor_data', {}).get('resources', {})
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

    def validate_permissions(self):
        if self.user_id == 0:
            raise exceptions.InvalidMessageException(
                    'Running shell commands with uid == 0 is not allowed')
        if self.group_id == 0:
            raise exceptions.InvalidMessageException(
                    'Running shell commands with gid == 0 is not allowed')
