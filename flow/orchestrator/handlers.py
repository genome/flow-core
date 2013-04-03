import logging
import time
import uuid

from flow.petri import SetTokenMessage, NotifyTransitionMessage
from flow.redisom import get_object

LOG = logging.getLogger(__name__)

class PetriSetTokenHandler(object):
    message_class = SetTokenMessage
    def __init__(self, redis=None, service_interfaces=None, queue_name=None):
        self.redis = redis
        self.service_interfaces = service_interfaces
        self.queue_name = queue_name

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


class PetriNotifyTransitionHandler(object):
    message_class = NotifyTransitionMessage
    def __init__(self, redis=None, service_interfaces=None, queue_name=None):
        self.redis = redis
        self.service_interfaces = service_interfaces
        self.queue_name = queue_name

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
