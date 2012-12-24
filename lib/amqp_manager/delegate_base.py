import logging

LOG = logging.getLogger(__name__)

class Delegate(object):
    _REQUIRED_DELEGATE_METHODS = []

    def __init__(self, delegates=[]):
        self.delegates = delegates
        self._unready_delegates = set(delegates)

        self._ready_callbacks = []

        for delegate in self.delegates:
            for required_method in self._REQUIRED_DELEGATE_METHODS:
                assert hasattr(delegate, required_method)
                assert callable(getattr(delegate, required_method))

            delegate.add_ready_callback(self._delegate_ready)

    def add_ready_callback(self, callback):
        self._ready_callbacks.append(callback)

    def _delegate_ready(self, delegate):
        self._unready_delegates.discard(delegate)
        self.notify_ready()

    def notify_ready(self):
        if self._ready_callbacks and not self._unready_delegates:
            for rc in self._ready_callbacks:
                rc(self)
