import abc


class IReporter(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __call__(self, query_info):
        raise NotImplementedError()
