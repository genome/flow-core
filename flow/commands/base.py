from abc import ABCMeta, abstractmethod
import logging

LOG = logging.getLogger(__name__)

class CommandBase(object):
    __metaclass__ = ABCMeta

    @staticmethod
    def annotate_parser(parser):
        raise NotImplementedError

    @abstractmethod
    def __call__(self, parsed_arguments):
        raise NotImplementedError

