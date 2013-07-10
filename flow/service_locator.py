import flow.interfaces
import injector
import pkg_resources

@injector.singleton
@injector.inject(i=injector.Injector)
class ServiceLocator(flow.interfaces.IServiceLocator):
    def __init__(self):
        self._services = {}

    def _get_missing_service(self, name):
        for ep in pkg_resources.iter_entry_points('flow.services', name):
            cls = ep.load()
            service = self.i.get(cls)
            self._services[ep.name] = service
            break
        return service

    def __getitem__(self, name):
        try:
            return self._services[name]
        except KeyError:
            return self._get_missing_service(name)

    def get(self, name, default=None):
        try:
            return self[name]
        except KeyError:
            return default
