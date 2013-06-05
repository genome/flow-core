from flow.configuration.settings.base import SettingsBase

import copy


class CacheSettings(SettingsBase):
    def __init__(self, settings=None):
        if settings is not None:
            self._settings = settings
        else:
            self._settings = {}

    def get(self, path, default=None):
        split_path = path.split('.')

        current_dict = self._settings
        for step in split_path:
            try:
                current_dict = current_dict[step]
            except KeyError:
                return default

        return current_dict


    def set(self, path, value):
        split_path = path.split('.')

        current_dict = self._settings

        for step in split_path[:-1]:
            try:
                current_dict = current_dict[step]
            except KeyError:
                # XXX create entire subpath
                new_value = {}
                current_dict[step] = new_value
                current_dict = new_value

        old_value = current_dict.get(split_path[-1])
        current_dict[split_path[-1]] = value

        return old_value

    def replace(self, d):
        self._settings = d

    def to_dict(self):
        return copy.copy(self._settings)
