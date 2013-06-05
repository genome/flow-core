from flow.configuration.settings.injector import setting
from flow.handler import Handler
from flow.orchestrator.messages import CreateTokenMessage, NotifyPlaceMessage
from flow.orchestrator.messages import NotifyTransitionMessage
from flow.redisom import get_object
from injector import inject

import flow.interfaces
import logging


LOG = logging.getLogger(__name__)

@inject(redis=flow.interfaces.IStorage,
        service_interfaces=flow.interfaces.IServiceLocator,
        queue_name=setting('orchestrator.create_token_queue'))
class PetriCreateTokenHandler(Handler):
    message_class = CreateTokenMessage

    def _handle_message(self, message):
        net = get_object(self.redis, message.net_key)

        create_token_kwargs = getattr(message, 'create_token_kwargs', {})

        return net.create_put_notify(message.place_idx,
                self.service_interfaces,
                color=message.color,
                color_group_idx=message.color_group_idx,
                data=getattr(message, 'data', {}))


@inject(redis=flow.interfaces.IStorage,
        service_interfaces=flow.interfaces.IServiceLocator,
        queue_name=setting('orchestrator.notify_place_queue'))
class PetriNotifyPlaceHandler(Handler):
    message_class = NotifyPlaceMessage

    def _handle_message(self, message):
        net = get_object(self.redis, message.net_key)
        return net.notify_place(message.place_idx, color=message.color,
                service_interfaces=self.service_interfaces)


@inject(redis=flow.interfaces.IStorage,
        service_interfaces=flow.interfaces.IServiceLocator,
        queue_name=setting('orchestrator.notify_transition_queue'))
class PetriNotifyTransitionHandler(Handler):
    message_class = NotifyTransitionMessage

    def _handle_message(self, message):
        net = get_object(self.redis, message.net_key)
        return net.notify_transition(message.transition_idx,
                message.place_idx, token_idx=message.token_idx,
                service_interfaces=self.service_interfaces)
