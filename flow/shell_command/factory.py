import logging


LOG = logging.getLogger(__name__)


def build_objects(definitions, module, default_class=None):
    objects = {}

    for name, definition in definitions.iteritems():
        try:
            objects[name] = build_object(definition, module, default_class)
        except:
            LOG.exception('Failed to build object %s: %s', name, definition)
            raise

    return objects


def build_object(definition, module, default_class=None):
    def_copy = dict(definition)
    class_name = def_copy.pop('class', default_class)
    cls = getattr(module, class_name)
    try:
        obj = cls(**def_copy)
    except TypeError:
        LOG.exception('Failed to instantiate class (%s) with args: %s',
                cls, definition)
        raise


    return obj
