from flow.petri_net.actions.base import BasicActionBase
from twisted.internet import defer

import datetime
import logging

LOG = logging.getLogger(__name__)


class ExpireNetAction(BasicActionBase):
    required_args = ['ttl_days']

    def execute(self, net, color_descriptor, active_tokens, service_interfaces):
        seconds = int(datetime.timedelta(
            int(self.args['ttl_days'])).total_seconds())
        LOG.debug('Expiring net keys in %r seconds', seconds)
        net.expire(seconds)
        return [], defer.succeed(None)
