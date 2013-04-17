from flow.configuration.settings.injector import setting
from flow.petri import SetTokenMessage, NotifyTransitionMessage
from flow.redisom import get_object
from injector import inject

import flow.interfaces
import logging


LOG = logging.getLogger(__name__)

@inject(redis=flow.interfaces.IStorage,
        service_interfaces=flow.interfaces.IServiceLocator,
        queue_name=setting('orchestrator.set_token_queue'))
class PetriSetTokenHandler(flow.interfaces.IHandler):
    message_class = SetTokenMessage

    def __call__(self, message):
        try:
            net = get_object(self.redis, message.net_key)
            net.set_token(message.place_idx, message.token_key,
                    service_interfaces=self.service_interfaces)
        except Exception as e:
            LOG.exception(
                    'Handler (%s) failed to add tokens to net %s place %d: %s'
                    % (self, message.net_key, message.place_idx, str(e)))
            raise e

@inject(redis=flow.interfaces.IStorage,
        service_interfaces=flow.interfaces.IServiceLocator,
        queue_name=setting('orchestrator.notify_transition_queue'))
class PetriNotifyTransitionHandler(flow.interfaces.IHandler):
    message_class = NotifyTransitionMessage

    def __call__(self, message):
        try:
            net = get_object(self.redis, message.net_key)
            net.notify_transition(message.transition_idx, message.place_idx,
                    service_interfaces=self.service_interfaces)
        except Exception as e:
            LOG.exception('Handler (%s) failed to execute transition %s'
                          ' on net %s: %s' % (self, message.transition_idx,
                              message.net_key, str(e)))
            raise e
