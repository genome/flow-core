from flow.shell_command.resource import ResourceException
from pythonlsf import lsf

import abc


class LSFResource(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, name, operator='>=', units=None):
        self.name = name
        self.operator = operator
        self.units = units

    @abc.abstractmethod
    def set_select_component(self, request, select_strings, resource_spec):
        raise NotImplementedError('set_select_component not implemented on %s'
                % self.__class__)

    @abc.abstractmethod
    def set_reserve_component(self, request, rusage_strings, resource_spec):
        raise NotImplementedError('set_reserve_component not implemented on %s'
                % self.__class__)

class LSFResourceIgnored(LSFResource):
    def __init__(self):
        pass

    def set_select_component(self, request, select_strings, resource_spec):
        pass

    def set_reserve_component(self, request, rusage_strings, resource_spec):
        pass

class LSFResourceViaString(LSFResource):
    def set_select_component(self, request, select_strings, resource_spec):
        value = resource_spec.value_in_units(self.units)
        select_strings[self.name] = '%s%s%s' % (self.name, self.operator, value)

    def set_reserve_component(self, request, rusage_strings, resource_spec):
        value = resource_spec.value_in_units(self.units)
        rusage_strings[self.name] = '%s=%s' % (self.name, value)

class LSFResourceDirectRequest(LSFResource):
    def set_select_component(self, request, select_strings, resource_spec):
        value = resource_spec.value_in_units(self.units)
        setattr(request, self.name, value)

    def set_reserve_component(self, request, rusage_strings, resource_spec):
        value = resource_spec.value_in_units(self.units)
        setattr(request, self.name, value)


class LSFLimit(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def set_limit(self, request, rlimits, resource_spec):
        raise NotImplementedError('set_limit not implemented on %s'
                % self.__class__)

class LSFrlimit(LSFLimit):
    def __init__(self, option_index, units=None):
        self.option_index = option_index
        self.units = units

    def set_limit(self, request, rlimits, resource_spec):
        value = resource_spec.value_in_units(self.units)
        rlimits[self.option_index] = value


SELECT_MAP = {
    'cores': LSFResourceDirectRequest('maxNumProcessors'),
    'memory': LSFResourceViaString(name='mem', units='MiB'),
    'temp_space': LSFResourceViaString(name='gtmp', units='GiB'),
}

RESERVE_MAP = {
    'cores': LSFResourceDirectRequest(name='numProcessors'),
    'memory': LSFResourceViaString(name='mem', units='MiB'),
    'temp_space': LSFResourceViaString(name='gtmp', units='GiB'),
}

LIMIT_MAP = {
    'cpu_time': LSFrlimit(option_index=lsf.LSF_RLIMIT_CPU, units='s'),
    'memory': LSFrlimit(option_index=lsf.LSF_RLIMIT_RSS, units='MiB'),
    'open_files': LSFrlimit(option_index=lsf.LSF_RLIMIT_NOFILE),
    'processes': LSFrlimit(option_index=lsf.LSF_RLIMIT_PROCESS),
    'stack_size': LSFrlimit(
        option_index=lsf.LSF_RLIMIT_STACK, units='KiB'),
    'threads': LSFrlimit(option_index=lsf.LSF_RLIMIT_THREAD),
    'virtual_memory': LSFrlimit(
        option_index=lsf.LSF_RLIMIT_VMEM, units='MiB'),
}


def set_request_resources(request, resources):
    select_strings = {}
    rusage_strings = {}
    for name, spec in resources.get('reserve', {}).iteritems():
        try:
            RESERVE_MAP[name].set_reserve_component(
                    request, rusage_strings, spec)
        except KeyError:
            raise ResourceException('Could not map rusage resource "%s"' % name)

    for name, spec in resources.get('request', {}).iteritems():
        try:
            SELECT_MAP[name].set_select_component(
                    request, select_strings, spec)
        except KeyError:
            raise ResourceException('Could not map select resource "%s"' % name)

    if select_strings or rusage_strings:
        request.options |= lsf.SUB_RES_REQ
        request.resReq = make_rusage_string(select_strings.values(),
                rusage_strings.values())

    request.rLimits = make_rLimits(request, resources.get('limit', {}))


def make_rLimits(request, limits):
    rlimits = [lsf.DEFAULT_RLIMIT] * lsf.LSF_RLIM_NLIMITS

    for name, spec in limits.iteritems():
        try:
            LIMIT_MAP[name].set_limit(request, rlimits, spec)
        except KeyError:
            raise ResourceException('Could not map rlimit resource "%s"' % name)

    return rlimits


def make_rusage_string(select_strings, rusage_strings):
    components = []
    if select_strings:
        components.append('select[%s]' % ' && '.join(select_strings))

    if rusage_strings:
        components.append('rusage[%s]' % ':'.join(rusage_strings))

    return str(' '.join(components))
