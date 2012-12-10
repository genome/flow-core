import logging

LOG = logging.getLogger(__name__)

class AMQPService(object):
    def __init__(self, connection_manager, *responders):
        self.connection_manager = connection_manager
        self.responders = responders

    def run(self):
        LOG.info("Starting AMQP service")
        for responder in self.responders:
            LOG.debug("Registering responder %s", responder)
            self.connection_manager.register_responder(responder)
        self.connection_manager.run()

    def stop(self):
        LOG.info("Stopping AMQP service")
        self.connection_manager.stop()
