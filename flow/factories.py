import pkg_resources

import logging

LOG = logging.getLogger(__name__)


__all__ = [
    'dictionary_factory'
    'general_factory',
]


def dictionary_factory(**kwargs):
    for key, val in kwargs.iteritems():
        if isinstance(val, dict):
            kwargs[key] = general_factory(**val)
        elif isinstance(val, list):
            kwargs[key] = [general_factory(**entry) for entry in val]
    return kwargs


def general_factory(factory_name=None, **kwargs):
    if not factory_name:
        LOG.debug('No factory name given to general_factory, returning %s',
                kwargs)
        return kwargs
    LOG.debug('Constructing factory_name: %s', factory_name)

    concrete_factory = get_concrete_factory(factory_name)

    for name, value in kwargs.iteritems():
        if isinstance(value, dict):
            LOG.debug('general_factory recursing into dict argument (%s): %s',
                    name, value)
            kwargs[name] = general_factory(**value)
        elif isinstance(value, list):
            LOG.debug('Found argument list in general_factory, iterating..')
            obj_list = []
            for entry in value:
                if isinstance(entry, dict):
                    obj_list.append(general_factory(**entry))
                else:
                    obj_list.append(entry)
            kwargs[name] = obj_list

    return concrete_factory(**kwargs)

def get_concrete_factory(factory_name, category='flow.factories'):
    for ep in pkg_resources.iter_entry_points(category, name=factory_name):
        LOG.debug('Found entry point %s for factory_name %s',
                ep, factory_name)
        try:
            return ep.load()
        except ImportError:
            raise ImportError("Couldn't import factory '%s' from entry point '%s'" %
                    (factory_name, ep))

    raise RuntimeError('Found no entry point for factory_name %s' %
            factory_name)
