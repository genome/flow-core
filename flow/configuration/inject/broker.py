from flow.brokers.blocking import BlockingAmqpBroker
from flow.amqp_broker import AmqpBroker

import flow.interfaces
import injector


class BrokerConfiguration(injector.Module):
    @injector.singleton
    @injector.provides(flow.interfaces.IBroker)
    def provide_broker(self):
        return self.__injector__.get(AmqpBroker)


class BlockingBrokerConfiguration(injector.Module):
    @injector.singleton
    @injector.provides(flow.interfaces.IBroker)
    def provide_broker(self):
        return self.__injector__.get(BlockingAmqpBroker)
