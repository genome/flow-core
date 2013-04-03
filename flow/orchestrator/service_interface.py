import logging

from flow.petri import NotifyTransitionMessage, SetTokenMessage

LOG = logging.getLogger(__name__)

class OrchestratorServiceInterface(object):
    def __init__(self, broker=None,
            set_token_exchange=None,
            set_token_routing_key=None,
            notify_transition_exchange=None,
            notify_transition_routing_key=None):

        self.broker = broker

        self.set_token_exchange            = set_token_exchange
        self.set_token_routing_key         = set_token_routing_key
        self.notify_transition_exchange    = notify_transition_exchange
        self.notify_transition_routing_key = notify_transition_routing_key

    def set_token(self, net_key, place_idx, token_key=None):
        message = SetTokenMessage(net_key=net_key, place_idx=place_idx,
                token_key=token_key)
        self.broker.publish(self.set_token_exchange,
                self.set_token_routing_key, message)

    def notify_transition(self, net_key, transition_idx, place_idx):
        message = NotifyTransitionMessage(
                net_key=net_key,
                transition_idx=transition_idx,
                place_idx=place_idx)
        self.broker.publish(self.notify_transition_exchange,
                self.notify_transition_routing_key, message)

    def place_entry_observed(self, packet):
        exchange    = packet['exchange']
        routing_key = packet['routing_key']
        body        = packet['body']

        self.broker.raw_publish(exchange, routing_key, body)
