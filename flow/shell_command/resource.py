from flow.shell_command import factory

import abc
import copy
import logging
import sys


LOG = logging.getLogger(__name__)


MODULE = sys.modules[__name__]


class ResourceException(Exception):
    pass


class ResourceType(object):
    def __init__(self, resource_class, **resource_args):
        self.base_dict = resource_args
        self.base_dict['class'] = resource_class

    def __call__(self, value):
        d = copy.copy(self.base_dict)
        d['value'] = value
        return factory.build_object(d, module=MODULE)


class Resource(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def value_in_units(self, units):
        raise NotImplementedError('Class %s does not implement value_in_units'
                % self.__class__)

    def __eq__(self, other):
        return (self.__class__ == other.__class__
                ) and (self.value == other.value_in_units(self.units))


class IntegerResource(Resource):
    def __init__(self, value):
        self.value = value

    def value_in_units(self, units):
        if units is None:
            return self.value
        else:
            raise ResourceException('Cannot convert None units to %s'
                    % units)



class StorageResource(Resource):
    def __init__(self, value, units):
        self.value = value
        self.units = units

    def value_in_units(self, units):
        if units == self.units:
            return self.value
        else:
            return convert_memory_value(self.value, self.units, units)


class TimeResource(Resource):
    def __init__(self, value, units):
        self.value = value
        self.units = units

    def value_in_units(self, units):
        if units == self.units:
            return self.value
        else:
            return convert_time_value(self.value, self.units, units)


MEMORY_UNITS = [
    'B',
    'KiB',
    'MiB',
    'GiB',
    'TiB',
    'PiB',
    'EiB',
    'ZiB',
]

def convert_memory_value(src_value, src_units, dest_units):
    try:
        dest_index = MEMORY_UNITS.index(dest_units)
    except ValueError:
        raise ResourceException('Illegal destination memory unit (%s) not in %s'
                % (dest_units, MEMORY_UNITS))

    try:
        src_index = MEMORY_UNITS.index(src_units)
    except ValueError:
        raise ResourceException('Illegal source memory unit (%s) not in %s'
                % (src_units, MEMORY_UNITS))

    if src_index > dest_index:
        dest_value = int(src_value) << (10 * (src_index - dest_index))
    elif dest_index > src_index:
        dest_value = int(src_value) >> (10 * (dest_index - src_index))
    else:
        dest_value = int(src_value)

    return dest_value


def convert_time_value(src_value, src_units, dest_units):
    if src_units != dest_units:
        raise ResourceException('Time unit conversion not supported.')
    return int(src_value)


def make_all_resource_objects(resource_strings, resource_types):
    result = {
        'limit': make_resource_objects(
            resource_strings.get('limit', {}), resource_types),
        'request': make_resource_objects(
            resource_strings.get('request', {}), resource_types),
        'reserve': make_resource_objects(
            resource_strings.get('reserve', {}), resource_types),
    }
    return result


def make_resource_objects(src_dict, resource_types):
    result = {}
    for name, value in src_dict.iteritems():
        result[name] = resource_types[name](value)
    return result


def make_resource_types(resource_definitions):
    return factory.build_objects(resource_definitions, MODULE,
            'ResourceType')
