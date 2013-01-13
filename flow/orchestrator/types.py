#!/usr/bin/env python

import redisom as rom
import time

__all__ = ['NodeBase', 'Flow', 'NodeFailedError', 'NodeAlreadyCompletedError',
           'Status', 'StartNode', 'StopNode']

# FIXME: collect service names
ORCHESTRATOR = "orchestrator"

def _timestamp(redis_ts):
    return redis_ts[0] + redis_ts[1] * 1e-6

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


class NodeBase(rom.Object):
    execute_timestamp = rom.Property(rom.Scalar)
    complete_timestamp = rom.Property(rom.Scalar)
    flow_key = rom.Property(rom.Scalar)
    indegree = rom.Property(rom.Scalar)
    name = rom.Property(rom.Scalar)
    status = rom.Property(rom.Scalar)
    successors = rom.Property(rom.Set)
    input_connections = rom.Property(rom.Hash, value_decoder=rom.json_dec,
                                     value_encoder=rom.json_enc)
    outputs = rom.Property(rom.Hash, value_decoder=rom.json_dec,
                           value_encoder=rom.json_enc)

    @property
    def duration(self):
        if not self.execute_timestamp.value:
            return None

        if not self.complete_timestamp.value:
            end = _timestamp(self._connection.time())
        else:
            end = float(self.complete_timestamp.value)

        beg = float(self.execute_timestamp.value)
        return end - beg

    @property
    def environment(self):
        local_proxy = getattr(self, 'hidden_environment', None)
        if local_proxy.value:
            return local_proxy

        if self.flow:
            return self.flow.environment

    @environment.setter
    def environment(self, value):
        self.hidden_environment.value = value


    @property
    def working_directory(self):
        local_proxy = getattr(self, 'hidden_working_directory', None)
        if local_proxy.value:
            return local_proxy

        if self.flow:
            return self.flow.working_directory

    @working_directory.setter
    def working_directory(self, value):
        self.hidden_working_directory.value = value


    @property
    def user_id(self):
        local_proxy = getattr(self, 'hidden_user_id', None)
        if local_proxy.value:
            return local_proxy

        if self.flow:
            return self.flow.user_id

    @user_id.setter
    def user_id(self, value):
        self.hidden_user_id.value = value

    @property
    def inputs(self):
        try:
            inp_conn = self.input_connections
            if not inp_conn:
                return None
        except:
            return None

        rv = {}
        for key, props in inp_conn.iteritems():
            node = rom.get_object(self.connection, key)
            outputs = node.outputs
            if props:
                vals = outputs.values(props.values())
                rv.update(zip(props.keys(), vals))
            else:
                rv.update(outputs)

        return rv

    @property
    def now(self):
        return _timestamp(self._connection.time())

    @property
    def flow(self):
        return rom.get_object(self._connection, self.flow_key.value)

    def execute(self, services):
        if self.execute_timestamp.setnx(self.now):
            print "Executing '%s' (key=%s)" % (str(self.name), self.key)
            if self.status != Status.cancelled:
                self._execute(services)
            else:
                self.fail(services)

    def complete(self, services):
        self.status = Status.success
        if self.complete_timestamp.setnx(self.now):
            for succ_idx in self.successors:
                node = self.flow.node(succ_idx)
                idg = node.indegree.increment(-1)
                if idg == 0:
                    services[ORCHESTRATOR].execute_node(node.key)
        else:
            raise NodeAlreadyCompletedError(self.key)

    def cancel(self, services):
        print "Cancelling", self.name
        self.status = Status.cancelled

    def fail(self, services):
        print "Failing", self.name
        self.status = Status.failure
        for succ_idx in self.successors:
            node = self.flow.node(succ_idx)
            node.cancel(services)
            if node.indegree.increment(-1) == 0:
                node.fail(services)


    def _execute(self, services):
        raise NotImplementedError("_execute not implemented in %s" %
                self.__class__.__name__)

class StartNode(NodeBase):
    @property
    def inputs(self):
        return self.outputs

    def _execute(self, services):
        self.flow.execute_timestamp.setnx(self.now)
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
        self.flow.fail(services)

    def cancel(self, services):
        self.flow.cancel(services)


class DataNode(NodeBase):
    outputs = rom.Property(rom.Hash)


class Flow(NodeBase):
    node_keys = rom.Property(rom.List)
    hidden_environment = rom.Property(rom.Hash)
    hidden_user_id = rom.Property(rom.Scalar)
    hidden_working_directory = rom.Property(rom.Scalar)

    @property
    def flow(self):
        flow_key = self.flow_key.value
        if not flow_key:
            return self
        return rom.get_object(self._connection, flow_key)


    def node(self, idx):
        key = self.node_keys[idx]
        if key:
            return rom.get_object(self._connection, key)

    def _execute(self, services):
        services[ORCHESTRATOR].execute_node(self.node_keys[0])


class SleepNode(NodeBase):
    sleep_time = rom.Property(rom.Scalar)

    def _execute(self, services):
        sleep_time = self.sleep_time.value
        if sleep_time:
            time.sleep(float(sleep_time))
        self.complete(services)
