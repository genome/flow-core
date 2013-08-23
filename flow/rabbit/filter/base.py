import abc


class IFilter(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __call__(self, query_info):
        raise NotImplementedError()

    @abc.abstractmethod
    def header(self):
        raise NotImplementedError()
