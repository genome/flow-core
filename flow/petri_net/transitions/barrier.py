from base import TransitionBase
from flow.petri_net import lua
from flow.petri_net.actions.base import BarrierActionBase
from flow.petri_net.actions.merge import BarrierMergeAction

import flow.redisom as rom
import logging


LOG = logging.getLogger(__file__)


class BarrierTransition(TransitionBase):
    ACTION_BASE_CLASS = BarrierActionBase
    DEFAULT_ACTION_CLASS = BarrierMergeAction

    _consume_tokens = rom.Script(lua.load('consume_tokens_barrier'))

    def consume_tokens(self, enabler, color_descriptor, color_marking_key,
            group_marking_key):

        color_group = color_descriptor.group

        active_tokens_key = self.active_tokens_key(color_descriptor)
        state_key = self.state_key(color_descriptor)
        arcs_in_key = self.arcs_in.key
        enablers_key = self.enablers.key

        keys = [state_key, active_tokens_key, arcs_in_key, color_marking_key,
                group_marking_key, enablers_key]
        args = [enabler, color_group.idx, color_group.begin, color_group.end]

        LOG.debug("Consume tokens: KEYS=%r, ARGS=%r", keys, args)
        rv = self._consume_tokens(keys=keys, args=args)
        LOG.debug("Consume tokens returned: %r", rv)

        return rv[0]

    def state_key(self, color_descriptor):
        return self.subkey("state", color_descriptor.group.idx)

    def active_tokens_key(self, color_descriptor):
        return self.subkey("active_tokens", color_descriptor.group.idx)
