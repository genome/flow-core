from abc import ABCMeta, abstractmethod, abstractproperty


class IBroker(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def publish(self, exchange_name, routing_key, message):
        """
        Returns a deferred that will callback once the message has been
        confirmed.  If the AMQP server rejects the message then the deferred
        will not callback (nor errback) and the program will exit.
        """

    @abstractmethod
    def register_handler(self, handler):
        """
        Register a handler to accept messages on a queue.  When a connection is
        made the listener will be set up for you and will deliver message_class
        objects to the handler's __call__ function.
        """


class IHandler(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def __call__(self, message):
        """
        Returns a deferred that will callback when the message has been
        completely handled, or will errback when the message cannot be
        handled.
        """

    @abstractproperty
    def message_class(self):
        pass


class IOrchestrator(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def create_token(self, net_key, place_idx,
            color, color_group_index, data=None):
        pass

    @abstractmethod
    def notify_place(self, net_key, place_idx, color):
        pass

    @abstractmethod
    def notify_transition(self, net_key, transition_idx, place_idx, token_idx):
        pass


class IServiceLocator(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def get(self, name, defaut=None):
        pass

    @abstractmethod
    def __getitem__(self, name):
        pass


class ISettings(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def get(self, path, defaut=None):
        pass

    @abstractmethod
    def __getitem__(self, path):
        pass


class IStorage(object):
    __metaclass__ = ABCMeta
