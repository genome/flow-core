#!/usr/bin/env python

from redisom import *
import subprocess


__all__ = ['NodeBase', 'Flow', 'NodeFailedError', 'NodeAlreadyCompletedError',
           'Status']


class Status(object):
    new = "new"
    dispatched = "dispatched"
    running = "running"
    success = "success"
    failure = "failure"

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

    @property
    def flow(self):
        return get_object(self._connection, self.flow_key.value)

    def execute(self, context):
        raise RuntimeError("execute not implemented in %s"
                           %self.__class__.__name__)
    def complete(self, services):
        if self.completed.setnx(1):
            for succ_idx in self.successors:
                node = self.flow.node(succ_idx)
                if node.indegree.increment(-1) == 0:
                    node.execute(services)
        else:
            raise NodeAlreadyCompletedError(self.key)


class Flow(NodeBase):
    node_keys = RedisList
    environment = RedisHash
    user_id = RedisScalar

    def node(self, idx):
        key = self.node_keys[idx]
        if key:
            return get_object(self._connection, key)
