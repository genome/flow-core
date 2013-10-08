from flow.commands.base import CommandBase
from flow.configuration.inject.broker import BrokerConfiguration
from flow.configuration.inject.redis_conf import RedisConfiguration
from flow.configuration.inject.service_locator import ServiceLocatorConfiguration
from flow.petri_net import future
from flow.petri_net.actions.base import BasicActionBase, BarrierActionBase
from flow.petri_net.actions.time import RecordTimeAction
from flow.petri_net.builder import Builder
from twisted.internet import defer
from flow.service_locator import ServiceLocator


import flow.interfaces
import injector
import sys


class SplitTransition(BasicActionBase):
    required_arguments = ['size']

    def execute(self, net, color_descriptor, active_tokens, service_interfaces):
        new_color_group = net.add_color_group(size=int(self.args['size']),
                parent_color=color_descriptor.color,
                parent_color_group_idx=color_descriptor.group.idx)

        begin = new_color_group.begin
        idx = new_color_group.idx
        tokens = [net.create_token(color=(begin + i), color_group_idx=idx)
                  for i in xrange(new_color_group.size)]

        return tokens, defer.succeed(None)

class JoinTransition(BarrierActionBase):
    def execute(self, net, color_descriptor, active_tokens, service_interfaces):
        token = net.create_token(color=color_descriptor.group.parent_color,
            color_group_idx=color_descriptor.group.parent_color_group_idx)
        return [token], defer.succeed(None)


@injector.inject(storage=flow.interfaces.IStorage,
        broker=flow.interfaces.IBroker,
        service_locator=ServiceLocator)
class BenchmarkCommand(CommandBase):
    injector_modules = [
            BrokerConfiguration,
            RedisConfiguration,
            ServiceLocatorConfiguration,
    ]


    @staticmethod
    def annotate_parser(parser):
        parser.add_argument('--groups', type=int, default=1)
        parser.add_argument('--size', type=int, default=50)

    def _execute(self, parsed_arguments):
        net, start_place = self.construct_net(parsed_arguments.groups,
                parsed_arguments.size)
        completion_deferred = self.listen_for_completion(net)

        completion_deferred.addCallback(self.print_runtime, net=net)
        completion_deferred.addCallback(self.delete_net, net=net)

        self.start_net(net, start_place)

        return completion_deferred


    def listen_for_completion(self, net):
        declare_deferred = self.broker.declare_queue(net.key, durable=False,
                exclusive=True)
        done_deferred = defer.Deferred()
        declare_deferred.addCallback(self._register_completion_handler,
                done_deferred=done_deferred, net_key=net.key)
        return done_deferred

    def _register_completion_handler(self, result, done_deferred, net_key):
        from flow_workflow.completion import MonitoringCompletionHandler
        self.completion_handler = MonitoringCompletionHandler(
                queue_name=net_key, done_deferred=done_deferred)
        self.broker.register_handler(self.completion_handler)
        return result

    def print_runtime(self, result, net):
        run_time = (float(net.variables['stop_time'])
                    - float(net.variables['start_time']))
        sys.stdout.write('%r' % run_time)
        sys.stdout.flush()
        return result

    def delete_net(self, result, net):
        net.delete()
        return result


    def construct_net(self, groups, size):
        future_net, start_place = self.future_net(groups, size)

        builder = Builder(self.storage)
        stored_net = builder.store(future_net, {}, {})
        start_place_index = builder.future_places[start_place]
        return stored_net, start_place_index

    def start_net(self, net, start_place):
        cg = net.add_color_group(1)
        orchestrator = self.service_locator['orchestrator']
        return orchestrator.create_token(net.key, start_place, cg.begin, cg.idx)


    def future_net(self, groups, size):
        future_net = future.FutureNet()

        start_place = future_net.add_place(name='start place')
        start_time_transition = future_net.add_basic_transition(
                name='start time', action=future.FutureAction(
                    RecordTimeAction, destination='start_time'))
        start_place.add_arc_out(start_time_transition)

        last_split_transition = start_time_transition
        for i in xrange(groups):
            split_transition = future_net.add_basic_transition(
                    name='split', action=future.FutureAction(
                        SplitTransition, size=size))
            future_net.bridge_transitions(last_split_transition,
                    split_transition)
            last_split_transition = split_transition

        worker_transition = future_net.add_basic_transition()
        future_net.bridge_transitions(last_split_transition, worker_transition)

        last_join_transition = worker_transition
        for i in xrange(groups):
            join_transition = future_net.add_barrier_transition(
                    name='join', action=future.FutureAction(
                        JoinTransition))
            future_net.bridge_transitions(last_join_transition, join_transition)
            last_join_transition = join_transition

        stop_time_transition = future_net.add_basic_transition(
                name='stop time', action=future.FutureAction(
                    RecordTimeAction, destination='stop_time'))
        future_net.bridge_transitions(last_join_transition,
                stop_time_transition)

        from flow_workflow.entities.workflow.actions import NotificationAction
        notify_transition = future_net.add_basic_transition(
                name='notify transition', action=future.FutureAction(
                    cls=NotificationAction, status='done'))

        future_net.bridge_transitions(stop_time_transition, notify_transition)

        return future_net, start_place
