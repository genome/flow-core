from abc import *

class CommandBase(object):
    __metaclass__ = ABCMeta

    default_logging_mode = 'default'

    @staticmethod
    def annotate_parser(parser):
        raise NotImplementedError

    @abstractmethod
    def __call__(self, parsed_arguments):
        raise NotImplementedError
