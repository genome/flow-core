from abc import ABCMeta, abstractmethod, abstractproperty
import collections
import injector
import logging

LOG = logging.getLogger(__name__)

class CommandBase(object):
    __metaclass__ = ABCMeta

    @abstractproperty
    def injector_modules(self):
        pass

    @staticmethod
    def annotate_parser(parser):
        raise NotImplementedError

    @abstractmethod
    def __call__(self, parsed_arguments):
        raise NotImplementedError
