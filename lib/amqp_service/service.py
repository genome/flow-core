import logging

LOG = logging.getLogger(__name__)

class AMQPService(object):
    def __init__(self, connection_manager, *responders):
        self.connection_manager = connection_manager
        self.responders = responders

    def run(self):
        for responder in self.responders:
            self.connection_manager.register_responder(responder)
        self.connection_manager.run()

    def stop(self):
        self.connection_manager.stop()
