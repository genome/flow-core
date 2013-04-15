import abc


_SENTINEL = object()


class SettingsBase(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def get(self, path, defaut=None):
        pass

    def __getitem__(self, path):
        result = self.get(path, _SENTINEL)
        if result is _SENTINEL:
            raise KeyError('Could not find path (%s) in config' % path)
        return result
