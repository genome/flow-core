import flow.petri.safenet as sn

class FakeOrchestrator(object):
    def __init__(self, conn):
        self.conn = conn
        self.services = {"orchestrator": self}

    def set_token(self, net_key, place_idx, token_key=''):
        net = sn.SafeNet(self.conn, net_key)
        net.set_token(place_idx, token_key, services=self.services)

    def notify_transition(self, net_key, trans_idx, place_idx):
        net = sn.SafeNet(self.conn, net_key)
        net.notify_transition(trans_idx, place_idx, services=self.services)
