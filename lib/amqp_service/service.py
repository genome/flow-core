import logging

LOG = logging.getLogger(__name__)

class AMQPService(object):
    def __init__(self, amqp_manager, *responders):
        self.amqp_manager = amqp_manager
        self.responders = responders

    def run(self):
        for responder in self.responders:
            self.amqp_manager.register_responder(responder)
        self.amqp_manager.run()

    def stop(self):
        self.amqp_manager.stop()
