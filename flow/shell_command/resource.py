class ResourceException(Exception):
    pass


class Resource(object):
    UNITS = None

    def __init__(self, value):
        self.value = value

    def value_in_units(self, units):
        if units is None:
            return self.value
        else:
            raise ResourceException('Cannot convert None units to %s'
                    % units)

    def __eq__(self, other):
        return (self.__class__ == other.__class__
                ) and (self.value == other.value)


class IntegerResource(Resource):
    pass


class StorageResource(Resource):
    UNITS = 'GiB'

    def value_in_units(self, units):
        if units == self.UNITS:
            return self.value
        else:
            return convert_memory_value(self.value, self.UNITS, units)


class TimeResource(Resource):
    UNITS = 's'

    def value_in_units(self, units):
        if units == self.UNITS:
            return self.value
        else:
            return convert_time_value(self.value, self.UNITS, units)


RESOURCE_CLASSES = {
        'cores': IntegerResource,
        'cpu_time': TimeResource,
        'open_files': IntegerResource,
        'memory': StorageResource,
        'processes': IntegerResource,
        'stack_size': IntegerResource,
        'temp_space': StorageResource,
        'threads': IntegerResource,
        'virtual_memory': StorageResource,
}


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


def make_all_resource_objects(resource_strings):
    result = {
        'limit': make_resource_objects(resource_strings.get('limit', {})),
        'request': make_resource_objects(resource_strings.get('request', {})),
        'reserve': make_resource_objects(resource_strings.get('reserve', {})),
    }
    return result

def make_resource_objects(src_dict):
    result = {}
    for name, value in src_dict.iteritems():
        result[name] = RESOURCE_CLASSES[name](value)
    return result
