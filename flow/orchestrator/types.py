#!/usr/bin/env python

from redisom import *
import json

__all__ = ['NodeBase', 'Flow', 'NodeFailedError', 'NodeAlreadyCompletedError',
           'Status', 'StartNode', 'StopNode']


class Status(object):
    new = "new"
    dispatched = "dispatched"
    running = "running"
    success = "success"
    failure = "failure"
    cancelled = "cancelled"

    _completed_values = set([success, failure])

    @staticmethod
    def done(status):
        return status in _completed_values


class NodeFailedError(RuntimeError):
    def __init__(self, node_key, msg):
        self.node_key = node_key
        RuntimeError.__init__(self, "Node %s failed: %s" %(node_key, msg))


class NodeAlreadyCompletedError(RuntimeError):
    def __init__(self, node_key):
        self.node_key = node_key
        RuntimeError.__init__(self, "Node %s already completed!" %node_key)


class NodeBase(RedisObject):
    flow_key = RedisScalar
    indegree = RedisScalar
    name = RedisScalar
    status = RedisScalar
    completed = RedisScalar
    successors = RedisSet
    input_connections = RedisHash
    outputs = RedisHash

    @property
    def inputs(self):
        try:
            inp_conn = self.input_connections
            if not inp_conn:
                return None
        except:
            return None

        rv = {}
        for idx, mapping_str in inp_conn.iteritems():
            idx = int(idx)
            props = json.loads(mapping_str)
            if props:
                vals = self.flow.node(idx).outputs.values(props.values())
                rv.update(zip(props.keys(), vals))
            else:
                rv.update(self.flow.node(idx).outputs.value)
        return rv

    @property
    def flow(self):
        return get_object(self._connection, self.flow_key.value)

    def execute(self, services):
        print "Executing '%s' (key=%s)" % (str(self.name), self.key)
        if self.status != Status.cancelled:
            self._execute(services)
        else:
            self.fail(services)

    def complete(self, services):
        if self.completed.setnx(1):
            for succ_idx in self.successors:
                node = self.flow.node(succ_idx)
                idg = node.indegree.increment(-1)
                if idg == 0:
                    node.execute(services)
        else:
            raise NodeAlreadyCompletedError(self.key)

    def cancel(self, services):
        print "Cancelling", self.name
        self.status = Status.cancelled

    def fail(self, services):
        print "Failing", self.name
        for succ_idx in self.successors:
            node = self.flow.node(succ_idx)
            node.cancel(services)
            if node.indegree.increment(-1) == 0:
                node.fail(services)


class StartNode(NodeBase):
    @property
    def inputs(self):
        return self.outputs

    def _execute(self, services):
        self.complete(services)


class StopNode(NodeBase):
    def _execute(self, services):
        inputs = self.inputs
        if inputs:
            self.flow.outputs = inputs
        self.complete(services)

    def complete(self, services):
        print "Completing a stop node (%s)!" % self.name
        self.flow.complete(services)

    def fail(self, services):
        self.flow.fail()

    def cancel(self, services):
        self.flow.cancel()


class Flow(NodeBase):
    node_keys = RedisList
    environment = RedisHash
    user_id = RedisScalar
    working_directory = RedisScalar

    def node(self, idx):
        key = self.node_keys[idx]
        if key:
            return get_object(self._connection, key)
