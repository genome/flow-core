from flow.configuration.inject import initialize

import abc
import flow.interfaces


SENTINEL = object()


class BaseSetting(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def _cannot_instantiate_settings_objects(self):
        '''
        This prevents users from accidentally asking for settings without
        registering a provider.
        '''


def setting(key, default=SENTINEL):
    new_class = type('Setting__%s__%s__' % (key, default),
            (BaseSetting,), {'key': key, 'default': default})

    initialize.INJECTOR.binder.bind(new_class, get_setting_factory(new_class))

    return new_class


def get_setting_factory(setting_class):
    def get_setting():
        settings = initialize.INJECTOR.get(flow.interfaces.ISettings)
        if setting_class.default is not SENTINEL:
            return settings.get(setting_class.key, setting_class.default)
        else:
            return settings[setting_class.key]

    return get_setting
