from flow.configuration.settings.base import SettingsBase


class PrioritySettings(SettingsBase):
    def __init__(self):
        self._settings_objects = []

    def get(self, path, default=None):
        for c in reversed(self._settings_objects):
            try:
                x = c[path]
                return x
            except KeyError:
                pass

        return default

    def append(self, settings_object):
        self._settings_objects.append(settings_object)

    def extend(self, settings_objects):
        self._settings_objects.extend(settings_objects)

    def clear(self):
        self._settings_objects = []
