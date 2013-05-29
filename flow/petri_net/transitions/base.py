from flow.petri_net import lua
from itertools import product
from twisted.internet import defer

import flow.redisom as rom
import logging


LOG = logging.getLogger(__file__)


class TransitionBase(rom.Object):
    arcs_in = rom.Property(rom.List, value_decoder=int, value_encoder=int)
    arcs_out = rom.Property(rom.List, value_decoder=int, value_encoder=int)

    enablers = rom.Property(rom.Hash)
    action_key = rom.Property(rom.String)

    tokens_pushed = rom.Property(rom.Int)

    _push_tokens_script = rom.Script(lua.load('push_tokens'))

    @property
    def action(self):
        action_key = None
        try:
            action_key = self.action_key.value
        except rom.NotInRedisError:
            return None

        if action_key:
            return rom.get_object(self.connection, action_key)
        else:
            return None

    def consume_tokens(self, enabler, color_descriptor, color_marking_key,
            group_marking_key):
        raise NotImplementedError()

    def fire(self, net, color_descriptor, service_interfaces):
        raise NotImplementedError()

    def push_tokens(self, net, color_descriptor, tokens):
        keys = [self.active_tokens(color_descriptor).key, self.arcs_out.key,
                net.color_marking.key, net.group_marking.key]

        args = [len(tokens)]
        for t in tokens:
            args.extend([t.color_group_idx.value, t.color.value, t.index.value])

        rv = self._push_tokens_script(keys=keys, args=args)
        LOG.debug("rv=%r", rv)
        return rv[0]

    def notify_places(self, net_key, colors, service_interfaces):
        deferreds = []

        orchestrator = service_interfaces['orchestrator']
        for place_idx, color in product(self.arcs_out, colors):
            deferred = orchestrator.notify_place(net_key, place_idx, color)
            deferreds.append(deferred)

        return defer.DeferredList(deferreds)

    def state_key(self, color_descriptor):
        raise NotImplementedError()

    def active_tokens_key(self, color_descriptor):
        raise NotImplementedError()

    def active_tokens(self, color_descriptor):
        return rom.Set(connection=self.connection,
                key=self.active_tokens_key(color_descriptor))


