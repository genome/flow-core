from pythonlsf import lsf

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
