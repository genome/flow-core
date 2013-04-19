from flow.service_locator import ServiceLocator

import flow.interfaces
import injector


class ServiceLocatorConfiguration(injector.Module):
    def configure(self, binder):
        binder.bind(flow.interfaces.IServiceLocator, ServiceLocator)
