from abc import ABCMeta, abstractmethod, abstractproperty
from twisted.internet import reactor, defer
import logging

LOG = logging.getLogger(__name__)

class CommandBase(object):
    __metaclass__ = ABCMeta

    def __init__(self, exit_code=0):
        self.exit_code = exit_code

    @abstractproperty
    def injector_modules(self):
        pass

    @staticmethod
    def annotate_parser(parser):
        raise NotImplementedError

    def _setup(self, parsed_arguments):
        """
        Prepare for entering the reactor's I/O loop by setting up any
        callbacks via reactor.callWhenRunning or other mechanisms.
        """
        reactor.callWhenRunning(self._execute_and_stop, parsed_arguments)

    @defer.inlineCallbacks
    def _execute_and_stop(self, parsed_arguments):
        try:
            yield self._execute(parsed_arguments)
        except Exception:
            LOG.exception("Unexpected Exception raised in command execution.")
        LOG.debug("Stopping the twisted reactor.")
        reactor.stop()

    @abstractmethod
    def _execute(self, parsed_arguments):
        """
        Returns a deferred that fires when it is okay to stop
        the reactor
        """
        raise NotImplementedError

    def _teardown(self, parsed_arguments):
        """
        Anything that should be done after the reactor has been
        stopped.
        """
        pass

    def execute(self, parsed_arguments):
        self._setup(parsed_arguments)
        reactor.run()
        self._teardown(parsed_arguments)
        return self.exit_code

