import flow.redisom as rom
import mock

class FakeOrchestrator(object):
    def __init__(self, conn):
        self.conn = conn
        self.service_interfaces = {"orchestrator": self}

        self.place_entry_observed = mock.Mock()

    def set_token(self, net_key, place_idx, token_key='', token_color=None):
        net = rom.get_object(self.conn, net_key)
        net.set_token(place_idx, token_key,
                service_interfaces=self.service_interfaces,
                token_color=token_color)

    def notify_transition(self, net_key, trans_idx, place_idx,
            token_color=None):
        net = rom.get_object(self.conn, net_key)
        net.notify_transition(trans_idx, place_idx,
                service_interfaces=self.service_interfaces,
                token_color=token_color)

