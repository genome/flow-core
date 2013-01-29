import argparse
from flow.commands.base import CommandBase
import logging
import time
import uuid

LOG = logging.getLogger()


class OrchestratorCommand(CommandBase):
    def __init__(self, storage=None, broker=None, handlers=None,
            service_interfaces={}):
        self.storage = storage
        self.broker = broker
        self.handlers = handlers
        self.service_interfaces = service_interfaces

    @staticmethod
    def annotate_parser(parser):
        pass

    def __call__(self, parsed_arguments):
        for service_name, client in self.service_interfaces.iteritems():
            client.broker = self.broker

        for handler in self.handlers:
            handler.services = self.service_interfaces
            handler.storage = self.storage

            self.broker.register_handler(handler)

            # TODO This should not be needed.  Fix up handlers and remove this
            handler.broker = self.broker

        self.broker.connect_and_listen()

        return 0
