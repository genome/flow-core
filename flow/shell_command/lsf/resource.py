from flow.shell_command.resource import Resource, ResourceException

_RESOURCE_MAP = {
        "min_proc": Resource(name="ncpus", type="int", units=None,
                operator=">=", reservable=False),

        "memory": Resource(name="mem", type="memory", units="MiB",
                operator=">=", reservable=True),

        "temp_space": Resource(name="gtmp", type="memory", units="GiB",
                operator=">=", reservable=True),
        }


def _select_item(name, value):
    resource = _RESOURCE_MAP[name]
    return "%s%s%s" % (resource.name, resource.operator, value)

def _rusage_item(name, value):
    resource = _RESOURCE_MAP[name]
    if not resource.reservable:
        raise ResourceException(
                "Attempted to reserve non-reservable resource %s" %
                resource.name)
    return "%s=%s" % (resource.name, value)

def make_rusage_string(require, reserve):
    select = []
    for k, v in require.iteritems():
        select.append(_select_item(k, v))

    rusage = []
    for k, v in reserve.iteritems():
        if k not in require:
            select.append(_select_item(k, v))
        rusage.append(_rusage_item(k, v))

    rv = []
    if select:
        rv.append("select[%s]" % " && ".join(select))

    if rusage:
        rv.append("rusage[%s]" % ":".join(rusage))

    # we do not want this to be unicode (LSF will crash)
    return str(" ".join(rv))
