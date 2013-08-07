from flow.configuration.settings.injector import setting
from flow.orchestrator.messages import CreateTokenMessage, NotifyPlaceMessage
from flow.orchestrator.messages import NotifyTransitionMessage
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
    def create_token(self, net_key, place_idx,
            color, color_group_idx, data=None):
        message = CreateTokenMessage(net_key=net_key, place_idx=place_idx,
                color=color, color_group_idx=color_group_idx, data=data)
        return self.broker.publish(self.create_token_exchange,
                self.create_token_routing_key, message)

    def notify_place(self, net_key, place_idx, color):
        message = NotifyPlaceMessage(net_key=net_key, place_idx=place_idx,
                color=color)
        return self.broker.publish(self.notify_place_exchange,
                self.notify_place_routing_key, message)

    def notify_transition(self, net_key, transition_idx, place_idx, token_idx):
        message = NotifyTransitionMessage(
                net_key=net_key,
                transition_idx=transition_idx,
                place_idx=place_idx,
                token_idx=token_idx)
        return self.broker.publish(self.notify_transition_exchange,
                self.notify_transition_routing_key, message)
