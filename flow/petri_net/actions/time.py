from flow.petri_net.actions.base import BasicActionBase
from twisted.internet import defer


class RecordTimeAction(BasicActionBase):
    required_arguments = ['destination']

    def execute(self, net, color_descriptor, active_tokens, service_interfaces):
        sec, usec = self.connection.time()
        net.variables[self.args['destination']] = '%d.%06d' % (sec, usec)
        return map(net.token, active_tokens), defer.succeed(None)
