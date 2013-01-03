#!/usr/bin/env python

from redisom import *
import subprocess


__all__ = ['Step', 'Node', 'StepFailedError', 'ShellCommandStep',
'PassThroughStep', 'Flow']

class StepFailedError(RuntimeError): pass

class Step(Storable):
    service_name = RedisScalar
    node_key = RedisScalar

    @property
    def node(self):
        return get_object(self._connection, self.node_key)

    def execute(self):
        raise NotImplementedError("Execute not implemented in step!")


class ShellCommandStep(Step):
    command_line = RedisList

    def execute(self):
        rv = subprocess.call(self.command_line.value)
        if rv != 0:
            raise StepFailedError("Command line step failed: '%s' returned %d"
                                  %(" ".join(self.command_line), rv))

class PassThroughStep(Step):
    data = RedisHash

    def execute(self):
        print "Pass through step returns", self.data
        self.node.outputs = self.data


class Node(Storable):
    flow_key = RedisScalar
    indegree = RedisScalar
    name = RedisScalar
    log_dir = RedisScalar
    status = RedisScalar
    step_keys = RedisList
    current_step = RedisScalar

    successors = RedisSet
    input_connections = RedisHash
    outputs = RedisHash

    @property
    def flow(self):
        return get_object(self._connection, self.flow_key.value)

    def step(self, idx):
        return get_object(self._connection, self.step_keys[idx])

    def execute_step(self, idx):
        print "Executing", self.name
        if len(self.step_keys) == 0:
            print "Node with no step!", self.name
            return []

        self.step(idx).execute()
        ready_nodes = []
        for succ_idx in self.successors:
            node = self.flow.node(succ_idx)
            if node.indegree.increment(-1) == 0:
                ready_nodes.append(node)
        return ready_nodes

class Flow(Storable):
    name = RedisScalar
    status = RedisScalar
    node_keys = RedisList

    def node(self, idx):
        key = self.node_keys[idx]
        if not key:
            return None
        return Node(self._connection, key)


if __name__ == "__main__":
    import redis
    c = redis.Redis()
    step = Step(connection=c)
    node = Node(connection=c, name="First Node")
    flow = Flow(connection=c)
    flow.node_keys

