from flow.shell_command.resource_types import ResourceException
from pythonlsf import lsf

import abc
import logging


LOG = logging.getLogger(__name__)


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
        rlimits[getattr(lsf, self.option_index)] = value


def set_all_resources(request, resources, available_resources):
    rusage_strings = set_reserve(request, resources.get('reserve', {}),
            available_resources.get('reserve', {}))

    select_strings = set_request(request, resources.get('request', {}),
            available_resources.get('request', {}))

    if select_strings or rusage_strings:
        request.options |= lsf.SUB_RES_REQ
        request.resReq = make_rusage_string(select_strings, rusage_strings)

    set_rlimits(request, resources.get('limit', {}),
            available_resources.get('limit', {}))


def set_reserve(request, resources, available_for_reserve):
    rusage_strings = {}
    for name, spec in resources.iteritems():
        try:
            available_for_reserve[name].set_reserve_component(
                    request, rusage_strings, spec)
        except KeyError:
            raise ResourceException('Could not map rusage resource "%s"' % name)

    return rusage_strings.values()


def set_request(request, resources, available_for_request):
    select_strings = {}
    for name, spec in resources.iteritems():
        try:
            available_for_request[name].set_select_component(
                    request, select_strings, spec)
        except KeyError:
            raise ResourceException('Could not map select resource "%s"' % name)

    return select_strings.values()


def set_rlimits(request, limits, available_limits):
    rlimits = [lsf.DEFAULT_RLIMIT] * lsf.LSF_RLIM_NLIMITS

    for name, spec in limits.iteritems():
        try:
            available_limits[name].set_limit(request, rlimits, spec)
        except KeyError:
            raise ResourceException('Could not map rlimit resource "%s"' % name)

    request.rLimits = rlimits


def make_rusage_string(select_strings, rusage_strings):
    components = []
    if select_strings:
        components.append('select[%s]' % ' && '.join(select_strings))

    if rusage_strings:
        components.append('rusage[%s]' % ':'.join(rusage_strings))

    return str(' '.join(components))
