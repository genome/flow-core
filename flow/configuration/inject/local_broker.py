from flow.brokers.local import LocalBroker

import flow.interfaces
import injector


class BrokerConfiguration(injector.Module):
    @injector.singleton
    @injector.provides(flow.interfaces.IBroker)
    def provide_broker(self):
        return self.__injector__.get(LocalBroker)
