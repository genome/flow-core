from collections import namedtuple


Resource = namedtuple("resource", "name type units operator reservable")


class ResourceException(Exception):
    pass
