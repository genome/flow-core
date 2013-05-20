from flow.configuration.settings.injector import setting
from flow.orchestrator.messages import CreateTokenMessage, NotifyPlaceMessage
from flow.orchestrator.messages import NotifyTransitionMessage
from flow.orchestrator.messages import PlaceEntryObservedMessage
from injector import inject

import flow.interfaces
import logging


LOG = logging.getLogger(__name__)


@inject(broker=flow.interfaces.IBroker,
        create_token_exchange=
            setting('orchestrator.create_token_exchange'),
        create_token_routing_key=
            setting('orchestrator.create_token_routing_key'),
        notify_place_exchange=
            setting('orchestrator.notify_place_exchange'),
        notify_place_routing_key=
            setting('orchestrator.notify_place_routing_key'),
        notify_transition_exchange=
            setting('orchestrator.notify_transition_exchange'),
        notify_transition_routing_key=
            setting('orchestrator.notify_transition_routing_key'))
class OrchestratorServiceInterface(flow.interfaces.IOrchestrator):
    def create_token(self, net_key, place_idx, **create_token_kwargs):
        message = CreateTokenMessage(net_key=net_key, place_idx=place_idx,
                create_token_kwargs=create_token_kwargs)
        return self.broker.publish(self.create_token_exchange,
                self.create_token_routing_key, message)

    def notify_place(self, net_key, place_idx, token_color):
        message = NotifyPlaceMessage(net_key=net_key, place_idx=place_idx,
                token_color=token_color)
        return self.broker.publish(self.notify_place_exchange,
                self.notify_place_routing_key, message)

    def notify_transition(self, net_key, transition_idx, place_idx,
            token_color=None):
        message = NotifyTransitionMessage(
                net_key=net_key,
                transition_idx=transition_idx,
                place_idx=place_idx,
                token_color=token_color)
        return self.broker.publish(self.notify_transition_exchange,
                self.notify_transition_routing_key, message)

    def place_entry_observed(self, packet):
        exchange    = packet['exchange']
        routing_key = packet['routing_key']
        body        = packet['body']

        message = PlaceEntryObservedMessage(body=body)
        return self.broker.publish(exchange, routing_key, message)
