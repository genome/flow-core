from flow.configuration.inject import settings

import abc


class BaseSetting(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def _cannot_instantiate_settings_objects(self):
        '''
        This prevents users from accidentally asking for settings without
        registering a provider.
        '''

def _create_new_class(key, default):
    new_class = type('Setting__%s__%s__' % (key, default),
            (BaseSetting,), {'key': key, 'default': default})

    settings.SETTING_REGISTRY[(key, default)] = new_class

    return new_class


def setting(key, default=settings.SENTINEL):
    try:
        return settings.SETTING_REGISTRY[(key, default)]
    except KeyError:
        return _create_new_class(key, default)
