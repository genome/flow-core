from twisted.internet import defer

import flow.interfaces
import flow.redisom as rom
import mock


class FakeOrchestrator(flow.interfaces.IOrchestrator):
    def __init__(self, conn):
        self.conn = conn
        self.service_interfaces = {"orchestrator": self}

        self.place_entry_observed = mock.Mock()

    def create_token(self, net_key, place_idx, **ct_kwargs):
        net = rom.get_object(self.conn, net_key)
        return net.create_put_notify(place_idx,
                service_interfaces=self.service_interfaces,
                **ct_kwargs)

    def notify_place(self, net_key, place_idx, token_color):
        net = rom.get_object(self.conn, net_key)
        return net.notify_place(place_idx, token_color=token_color,
                service_interfaces=self.service_interfaces)

    def notify_transition(self, net_key, trans_idx, place_idx,
            token_color=None):
        net = rom.get_object(self.conn, net_key)
        return net.notify_transition(trans_idx, place_idx,
                service_interfaces=self.service_interfaces,
                token_color=token_color)

    def place_entry_observed(self, packet):
        return defer.succeed(True)
