class Dispatcher:
    def __call__(self, node):
        raise NotImplementedError("__call__ not implemented in %s"
                                  %self.__class__.__name__)

class InlineDispatcher:
    def __call__(self, node):
        map(self, node.execute_step(0))
