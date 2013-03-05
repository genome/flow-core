from collections import namedtuple
import inspect

_RESOURCE_NAMES_AND_DEFAULTS = {
    "core_file_size": None,
    "cpu_time": None,
    "data_size": None,
    "file_size": None,
    "open_files": None,
    "stack_size": None,
    "virtual_memory": None,
    "wallclock_time": None
}

_ResourceLimits = namedtuple("ResourceLimits",
        " ".join(_RESOURCE_NAMES_AND_DEFAULTS.keys()))

def ResourceLimits(**kwargs):
    params = dict(_RESOURCE_NAMES_AND_DEFAULTS)
    params.update(kwargs)
    return _ResourceLimits(**params)
