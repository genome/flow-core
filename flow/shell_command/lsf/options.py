from flow.configuration.settings.injector import setting
from flow.shell_command import factory
from pythonlsf import lsf

import injector
import sys


MODULE = sys.modules[__name__]


TYPE_MAP = {
    'float': float,
    'int': int,
    'str': str,
}


class LSFOption(object):
    def __init__(self, name, flag=None, type='str', option_set=''):
        self.name = str(name)
        if flag:
            self.flag = getattr(lsf, flag)
        else:
            self.flag = None
        self.type = TYPE_MAP[type]
        self.options_name = 'options%s' % option_set

    def set_option(self, request, value):
        cast_value = self.type(value)
        setattr(request, self.name, cast_value)

        if self.flag is not None:
            options = getattr(request, self.options_name)
            setattr(request, self.options_name, options | int(self.flag))


@injector.inject(
    option_definitions=setting('shell_command.lsf.available_options'),
    default_options=setting('shell_command.lsf.default_options'))
class LSFOptionManager(object):
    def __init__(self):
        self.available_options = factory.build_objects(
                self.option_definitions, MODULE, 'LSFOption')

    def set_default_options(self, request):
        for option, value in self.default_options.iteritems():
            self.available_options[option].set_option(request, value)

    def set_options(self, request, executor_data):
        self.set_default_options(request)

        lsf_options = executor_data.get('lsf_options', {})

        for name, value in lsf_options.iteritems():
            self.available_options[name].set_option(request, value)

        for name in ('stderr', 'stdout', 'stdin'):
            value = executor_data.get(name)
            if value:
                self.available_options[name].set_option(request, value)
