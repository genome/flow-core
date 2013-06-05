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
        except:
            LOG.exception('Unexpected error handling message. message = %s',
                    message)
            deferred = defer.fail(None)
        return deferred

    @abstractmethod
    def _handle_message(self, message):
        """
        Returns a deferred to __call__
        """
