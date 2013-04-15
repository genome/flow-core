from flow.brokers.blocking import BlockingAmqpBroker
from flow.brokers.strategic_broker import StrategicAmqpBroker
from flow.brokers.acking_strategies import PublisherConfirmation

import flow.interfaces
import injector

class BrokerConfiguration(injector.Module):
    @injector.singleton
    @injector.provides(flow.interfaces.IBroker)
    def provide_broker(self):
        return self.__injector__.get(StrategicAmqpBroker)

class BlockingBrokerConfiguration(injector.Module):
    def configure(self, binder):
        binder.bind(flow.interfaces.IBroker, BlockingAmqpBroker)
