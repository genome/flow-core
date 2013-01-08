#!/usr/bin/env python

from redisom import *
import subprocess


__all__ = ['Node', 'Flow', 'NodeFailedError']

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
        RuntimeError.__init__(self, "Node %s failed: %s" %(node_key, msg)


class NodeAlreadyCompletedError(RuntimeError):
    def __init__(self, node_key):
        self.node_key = node_key
        RuntimeError.__init__(self, "Node %s already completed!" %node_key)


class Node(Storable):
    flow_key = RedisScalar
    indegree = RedisScalar
    name = RedisScalar
    log_dir = RedisScalar
    status = RedisScalar
    completed = RedisScalar

    successors = RedisSet
    input_connections = RedisHash
    outputs = RedisHash

    @property
    def flow(self):
        return get_object(self._connection, self.flow_key.value)

    def execute(self, context):
        raise RuntimeError("execute not implemented in %s"
                           %self.__class__.__name__)
    def complete(self):
        ready_nodes = []
        if self.completed.setnx(1):
            for succ_idx in self.successors:
                node = self.flow.node(succ_idx)
                if node.indegree.increment(-1) == 0:
                    ready_nodes.append(node)
        else:
            raise NodeAlreadyCompletedError(self.key)

        return ready_nodes


class Flow(Storable):
    name = RedisScalar
    status = RedisScalar
    node_keys = RedisList
    environment = RedisHash
    user_id = RedisScalar

    def node(self, idx):
        key = self.node_keys[idx]
        if key:
            return get_object(self._connection, key)
