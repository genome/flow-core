import flow.redisom as rom
from twisted.internet import defer

class TransitionBase(rom.Object):
    arcs_in = rom.Property(rom.List)
    arcs_out = rom.Property(rom.List)

    enablers = rom.Property(rom.Hash)

    def notify(self, net, place_idx, color, service_interfaces):
        raise NotImplementedError()

    def fire(self, net, color, service_interfaces):
        raise NotImplementedError()

    def push_tokens(self, net, tokens, color, service_interfaces):
        raise NotImplementedError()

    def notify_places(self, net, color, service_interfaces):
        deferreds = []

        orchestrator = service_interfaces['orchestrator']
        for place_idx in self.arcs_out:
            deferred = orchestrator.notify_place(net.key, place_idx, color)
            deferreds.append(deferred)

        return defer.DeferredList(deferreds)

    def state_key(self, tag):
        return self.subkey("state", tag)

    def active_tokens_key(self, tag):
        return self.subkey("active_tokens", tag)

    def active_tokens(self, tag):
        return rom.List(connection=self.connection,
                key=self.active_tokens_key(tag))
