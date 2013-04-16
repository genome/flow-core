import injector
import pkg_resources

@injector.singleton
@injector.inject(i=injector.Injector)
class ServiceLocator(object):
    def __init__(self):
        self._services = {}
        for ep in pkg_resources.iter_entry_points('flow.services'):
            cls = ep.load()
            self._services[ep.name] = self.i.get(cls)

    def __getitem__(self, name):
        return self._services[name]
