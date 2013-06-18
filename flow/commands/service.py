from flow.commands.base import CommandBase
from injector import inject, Injector
from twisted.internet import defer

import flow.interfaces
import logging

LOG = logging.getLogger(__name__)


@inject(storage=flow.interfaces.IStorage, broker=flow.interfaces.IBroker,
        injector=Injector)
class ServiceCommand(CommandBase):
    def _setup(self, parsed_arguments):
        for handler in self.handlers:
            self.broker.register_handler(handler)

    def _execute(self, parsed_arguments):
        """
        Returns a deferred that will never fire.
        """
        deferred = defer.Deferred()
        return deferred
