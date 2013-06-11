from pythonlsf import lsf


class LSFOption(object):
    def __init__(self, name, flag=None, type=str, option_set=''):
        self.name = str(name)
        self.flag = flag
        self.type = type
        self.options_name = 'options%s' % option_set

    def set_option(self, request, value):
        cast_value = self.type(value)
        setattr(request, self.name, cast_value)

        if self.flag is not None:
            options = getattr(request, self.options_name)
            setattr(request, self.options_name, options | int(self.flag))


AVAILABLE_OPTIONS = {
    'beginTime': LSFOption(name='beginTime', type=int),
    'lsf_job_group': LSFOption(name='group',
        flag=lsf.SUB2_JOB_GROUP, option_set=2),
    'lsf_job_name': LSFOption(name='name', flag=lsf.SUB_JOB_NAME),
    'lsf_project': LSFOption(name='projectName', flag=lsf.SUB_PROJECT_NAME),
    'mail_user': LSFOption(name='mail_user', flag=lsf.SUB_MAIL_USER),
    'queue': LSFOption(name='queue', flag=lsf.SUB_QUEUE),
    'stderr': LSFOption(name='errFile', flag=lsf.SUB_ERR_FILE),
    'stdin': LSFOption(name='inFile', flag=lsf.SUB_IN_FILE),
    'stdout': LSFOption(name='outFile', flag=lsf.SUB_OUT_FILE),
    'termTime': LSFOption(name='termTime', type=int),
}
