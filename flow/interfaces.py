from abc import ABCMeta, abstractmethod, abstractproperty


class IBroker(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def publish(self, exchange_name, routing_key, message):
        pass

    @abstractmethod
    def raw_publish(self, exchange_name, routing_key, encoded_message):
        pass

    @abstractmethod
    def register_handler(self, handler):
        pass


class IHandler(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def __call__(self, message):
        pass

    @abstractproperty
    def message_class(self):
        pass


class IOrchestrator(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def set_token(self, net_key, place_idx, token_key=None):
        pass

    @abstractmethod
    def notify_transition(self, net_key, transition_idx, place_idx):
        pass

    @abstractmethod
    def place_entry_observed(self, packet):
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


class IShellCommand(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def submit(self, command_line, net_key=None, response_places=None,
            **executor_options):
        pass


class IShellCommandExecutor(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def execute(self, command_line, **kwargs):
        pass

    @abstractmethod
    def __call__(self, command_line, group_id=None, user_id=None,
            environment={}, **kwargs):
        pass


class IStorage(object):
    __metaclass__ = ABCMeta
