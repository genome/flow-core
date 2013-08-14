from flow.petri_net import lua
from flow.redisom import NotInRedisError
from itertools import product
from twisted.internet import defer

import flow.redisom as rom
import logging


LOG = logging.getLogger(__file__)


class TransitionBase(rom.Object):
    arcs_in = rom.Property(rom.List, value_decoder=int, value_encoder=int)
    arcs_out = rom.Property(rom.List, value_decoder=int, value_encoder=int)

    index = rom.Property(rom.Int)
    name = rom.Property(rom.String)

    enablers = rom.Property(rom.Hash)

    _push_tokens_script = rom.Script(lua.load('push_tokens'))

    def additional_associated_iterkeys(self):
        action = self.action
        if action is not None:
            yield action.key
            for key in action.associated_iterkeys():
                yield key

    def consume_tokens(self, enabler, color_descriptor, color_marking_key,
            group_marking_key):
        raise NotImplementedError()

    def state_key(self, color_descriptor):
        raise NotImplementedError()

    def active_tokens_key(self, color_descriptor):
        raise NotImplementedError()


    @property
    def action_key(self):
        return self.subkey('action')

    @property
    def action(self):
        try:
            action = rom.get_object(self.connection, self.action_key)
            assert isinstance(action, self.ACTION_BASE_CLASS)
            return action
        except NotInRedisError:
            return


    def fire(self, net, color_descriptor, service_interfaces):
        active_tokens = self.active_tokens(color_descriptor)
        action = self.action
        if action is None:
            action = self.DEFAULT_ACTION_CLASS(self.connection, self.action_key)

        return action.execute(net=net, active_tokens=active_tokens,
                color_descriptor=color_descriptor,
                service_interfaces=service_interfaces)

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

    def active_tokens(self, color_descriptor):
        return rom.Set(connection=self.connection,
                key=self.active_tokens_key(color_descriptor))


    def set_action(self, cls, args=None):
        return cls.create(self.connection, self.action_key, args=args)
