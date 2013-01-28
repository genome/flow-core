class Service(object):
    def __init__(self, storage=None, broker=None, handlers=None,
            service_interfaces={}):
        self.storage = storage
        self.broker = broker
        self.handlers = handlers
        self.service_interfaces = service_interfaces

    def run(self):
        for service_name, client in self.service_interfaces.iteritems():
            client.broker = self.broker

        for handler in self.handlers:
            handler.services = self.service_interfaces
            handler.storage = self.storage

            self.broker.register_handler(handler)

            # TODO This should not be needed.  Fix up handlers and remove this
            handler.broker = self.broker

        self.broker.connect_and_listen()
