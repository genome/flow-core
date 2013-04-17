import injector


SENTINEL = object()
SETTING_REGISTRY = {}


class InjectedSettings(injector.Module):
    def __init__(self, settings):
        self.settings = settings
        injector.Module.__init__(self)

    def configure(self, binder):
        for setting_class in SETTING_REGISTRY.itervalues():
            binder.bind(setting_class,
                    get_setting_factory(setting_class, self.settings))


def get_setting_factory(setting_class, settings):
    def get_setting():
        if setting_class.default is not SENTINEL:
            return settings.get(
                    setting_class.key, setting_class.default)
        else:
            return settings[setting_class.key]

    return get_setting
