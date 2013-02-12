#!/usr/bin/env python

from flow import redisom as rom
import time
import logging

__all__ = ['NodeBase', 'Flow', 'NodeFailedError', 'NodeAlreadyCompletedError',
           'Status', 'StartNode', 'StopNode', 'DataNode']

# FIXME: collect service names
ORCHESTRATOR = "orchestrator"

LOG = logging.getLogger(__name__)

class Status(object):
    new = "new"
    dispatched = "dispatched"
    running = "running"
    success = "success"
    failure = "failure"
    cancelled = "cancelled"

    _completed_values = set([success, failure, cancelled])

    @staticmethod
    def done(status):
        return status in Status._completed_values


class NodeFailedError(RuntimeError):
    def __init__(self, node_key, msg):
        self.node_key = node_key
        RuntimeError.__init__(self, "Node %s failed: %s" %(node_key, msg))


class NodeAlreadyCompletedError(RuntimeError):
    def __init__(self, node_key):
        self.node_key = node_key
        RuntimeError.__init__(self, "Node %s already completed!" % node_key)


class InheritedProperty(rom.Property):
    @staticmethod
    def make_property(name):
        private_name = rom._make_private_name(name)
        def getter(self):
            v = getattr(self, private_name)
            if v and v.value:
                return v
            if self.flow and self.key != self.flow.key:
                return getattr(self.flow, name)

        def setter(self, value):
            getattr(self, private_name).value = value

        def deleter(self):
            getattr(self, private_name).delete()
            # Unlike normal rom properties, we don't remove the accessor
            # when deleting an inherited property

        return property(getter, setter, deleter)


class NodeBase(rom.Object):
    flow_key = rom.Property(rom.Scalar)
    indegree = rom.Property(rom.Scalar)
    name = rom.Property(rom.Scalar)
    status = rom.Property(rom.Scalar)
    successors = rom.Property(rom.Hash)
    input_connections = rom.Property(rom.Hash, value_decoder=rom.json_dec,
                                     value_encoder=rom.json_enc)
    outputs = rom.Property(rom.Hash, value_decoder=rom.json_dec,
                           value_encoder=rom.json_enc)

    environment = InheritedProperty(rom.Hash)
    user_id = InheritedProperty(rom.Scalar)
    working_directory = InheritedProperty(rom.Scalar)

    @property
    def inputs(self):
        inp_conn = self.input_connections.value
        if not inp_conn:
            return None

        rv = {}
        for key, props in inp_conn.iteritems():
            node = rom.get_object(self._connection, key)
            outputs = node.outputs
            if props:
                for prop_key, prop_value in props.iteritems():
                    rv[prop_key] = outputs[prop_value]
            else:
                rv.update(outputs)

        return rv

    def _get_flow(self, default=None):
        if not self.flow_key or not self.flow_key.value:
            return default

        return rom.get_object(self._connection, self.flow_key.value)

    @property
    def flow(self):
        return self._get_flow()


class StartNode(NodeBase):
    fsm = {
        "states": [fsm.State("completed", final=True)],
        "initial_state": "completed",
        "transitions": {}
    }

    @property
    def outputs(self):
        return self.flow.inputs


class StopNode(NodeBase):
    fsm = {
        "states": [
            fsm.State("new", final=True),
            fsm.State("success", final=True),
            fsm.State("failure", final=True),
        ],
        "initial_state": "new",
        "transitions": {
            fsm.Transition(src="new", dst="success", event="success",
                           method="on_success"),
            fsm.Transition(src="new", dst="failure", event="failure",
                           method="on_failure"),
        }
    }

    def on_success(self, services):

    def _execute(self, services):
        inputs = self.inputs
        if inputs:
            self.flow.outputs = inputs
        self.complete(services)

    def complete(self, services):
        NodeBase.complete(self, services)
        LOG.debug("Completing a stop node (%s, for %s)!" % (self.name, self.flow.name))
        self.flow.complete(services)

    def fail(self, services):
        LOG.debug("Failing a stop node (%s, for %s)!" % (self.name, self.flow.name))
        NodeBase.fail(self, services)
        self.flow.fail(services)

    def cancel(self, services):
        LOG.debug("Canceling a stop node (%s, for %s)!" % (self.name, self.flow.name))
        NodeBase.fail(self, services)
        self.flow.fail(services)


class DataNode(NodeBase):
    status = Status.success


class Flow(NodeBase):
    node_keys = rom.Property(rom.List)

    @property
    def flow(self):
        return self._get_flow(default=self)

    def node(self, idx):
        key = self.node_keys[idx]
        if key:
            return rom.get_object(self._connection, key)

    def _execute(self, services):
        services[ORCHESTRATOR].execute_node(self.node_keys[0])

    def add_node(self, node):
        LOG.debug("Added node '%s' (key=%s) to flow '%s'" %
                (node.name, node.key, self.name))
        node.flow_key = self.key
        return self.node_keys.append(node.key)

    def add_nodes(self, nodes):
        # optimized for Redis (instead of just calling self.add_node in a loop)
        keys = []
        for n in nodes:
            n.flow_key = self.key
            keys.append(n.key)
        LOG.debug("Added %d node(s) to flow '%s'" %
                (len(keys), self.name))
        return self.node_keys.extend(keys)


class SleepNode(NodeBase):
    sleep_time = rom.Property(rom.Scalar)

    def _execute(self, services):
        sleep_time = self.sleep_time.value
        if sleep_time:
            time.sleep(float(sleep_time))
        self.complete(services)

