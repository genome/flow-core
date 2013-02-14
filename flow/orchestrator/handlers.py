import logging
import time
import uuid

from flow.petri.safenet import SafeNet
from flow.redisom import get_object, invoke_instance_method

LOG = logging.getLogger(__name__)

class PetriSetTokenHandler(object):
    def __init__(self, redis=None, services=None, queue_name=None):
        self.redis = redis
        self.services = services
        self.queue_name = queue_name

    def __call__(self, message):
        try:
            net = SafeNet(self.redis, message.net_key)
            net.set_token(message.place_idx, message.token_key,
                    services=self.services)
        except Exception as e:
            LOG.exception(
                    'Handler (%s) failed to add tokens to net %s place %d: %s'
                    % (self, message.net_key, message.place_idx, str(e)))
            raise e


class PetriNotifyTransitionHandler(object):
    def __init__(self, redis=None, services=None, queue_name=None):
        self.redis = redis
        self.services = services
        self.queue_name = queue_name

    def __call__(self, message):
        try:
            net = SafeNet(self.redis, message.net_key)
            net.notify_transition(message.transition_idx, message.place_idx,
                    services=self.services)
        except Exception as e:
            LOG.exception('Handler (%s) failed to execute transition %s: %s' %
                         (self, message.transition_idx, str(e)))
            raise e
