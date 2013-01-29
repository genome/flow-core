from flow.commands.base import CommandBase

import argparse
import logging
import flow.orchestrator.types as nodes
import flow.orchestrator.redisom as rom
import time
from flow.orchestrator.messages import ExecuteNodeMessage

LOG = logging.getLogger()


class BenchmarkCommand(CommandBase):
    default_logging_mode = 'silent'

    def __init__(self, broker=None, status_getter=None, storage=None,
            execute_node_routing_key=None):
        self.storage = storage
        self.broker = broker
        self.status_getter = status_getter

        self.execute_node_routing_key = execute_node_routing_key

        status_getter.broker = broker

    @staticmethod
    def annotate_parser(parser):
        parser.add_argument("--num-flows", default=1, type=int,
                            help="The total number of flows to construct")
        parser.add_argument("--nodes-each", default=100, type=int,
                            help="The number of nodes in each flow")
        parser.add_argument("--sleep-time", default=0, type=float,
                            help="Time in seconds each node should sleep")

    def __call__(self, parsed_arguments):
        exit_code = 1

        print "num_flows=%d" % parsed_arguments.num_flows
        print "nodes_each=%d" % parsed_arguments.nodes_each
        print "sleep_time=%f" % parsed_arguments.sleep_time

        beg = time.time()
        master_flow = build_all_flows(self.storage, parsed_arguments)
        end = time.time()
        print "master_flow=%s" % str(master_flow.key)
        print "construct_sec=%f" % (end - beg)

        message = ExecuteNodeMessage(node_key=master_flow.key)

        self.broker.connect()
        try:
            self.broker.publish(self.execute_node_routing_key, message)
            self.status_getter.create_queue()
            self.status_getter.block_until_status(master_flow.key,
                    [nodes.Status.success, nodes.Status.failure])
            status = master_flow.status.value
            if status == nodes.Status.success:
                print "execute_sec: %f seconds" % master_flow.duration
                exit_code = 0
            else:
                print "Flow execution failed: %s (%s)" % (status, type(status))
        except KeyboardInterrupt:
            self.broker.disconnect()

        return exit_code


def build_flow(conn, size, sleep_time=None, **kwargs):
    flow = nodes.Flow.create(connection=conn, **kwargs)
    start_node = nodes.StartNode.create(connection=conn, flow_key=flow.key,
                                        name="start node")
    stop_node = nodes.StopNode.create(connection=conn,
                                      flow_key=flow.key,
                                      indegree=size,
                                      name="stop node")

    node_indices = []
    node_keys = [start_node.key, stop_node.key]
    for i in xrange(size):
        name = "Node %d" % i
        node = nodes.SleepNode.create(
                connection=conn, name=name,
                flow_key=flow.key,
                indegree=1,
                sleep_time=sleep_time,
                successors=set([1]),
                )
        index = len(node_keys)
        node_indices.append(index)
        node_keys.append(node.key)

    flow.node_keys.value = node_keys
    start_node.successors.value = node_indices

    return flow


def build_all_flows(conn, args):
    master_flow = nodes.Flow.create(connection=conn, name="Benchmark flow")
    start_node = nodes.StartNode.create(connection=conn, name="start")
    stop_node = nodes.StopNode.create(connection=conn, name="stop",
                                      indegree=args.num_flows)
    child_flows = []
    child_flow_successors = set([1]) # stop node is index 1

    first_idx = master_flow.add_nodes([start_node, stop_node])
    for i in xrange(args.num_flows):
        child_flow = build_flow(conn, args.nodes_each,
                                sleep_time=args.sleep_time,
                                name="Test Workflow %d" % i)
        child_flow.indegree = 1
        child_flow.successors = child_flow_successors
        child_flows.append(child_flow)
    master_flow.add_nodes(child_flows)
    start_node.successors = set([x+first_idx for x in xrange(args.num_flows)])

    return master_flow
