from abc import abstractmethod
from flow.interfaces import IHandler
from twisted.internet import defer

import logging


LOG = logging.getLogger(__name__)


class Handler(IHandler):
    def __call__(self, message):
        """
        Returns a deferred that will callback when the message has been
        completely handled, or will errback when the message cannot be
        handled.
        """
        try:
            deferred = self._handle_message(message)
        except Exception as e:
            LOG.exception('Unexpected exception in handler (%r)', self)
            deferred = defer.fail(e)
        return deferred

    @abstractmethod
    def _handle_message(self, message):
        """
        Returns a deferred to __call__
        """

    def __repr__(self):
        return "%s(queue_name=%s)" % (self.__class__.__name__, self.queue_name)
