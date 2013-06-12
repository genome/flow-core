import logging


LOG = logging.getLogger(__name__)


def build_objects(definitions, module, default_class=None):
    objects = {}

    for name, definition in definitions.iteritems():
        objects[name] = build_object(definition, module, default_class)

    return objects


def build_object(definition, module, default_class=None):
    class_name = definition.pop('class', default_class)
    cls = getattr(module, class_name)
    try:
        obj = cls(**definition)
    except TypeError:
        LOG.exception('Failed to instantiate class (%s) with args: %s',
                cls, definition)
        raise


    return obj
