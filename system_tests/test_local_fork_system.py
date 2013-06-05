import unittest
from test_helpers import redistest

from flow.brokers.local import LocalBroker
from flow.orchestrator.service_interface import OrchestratorServiceInterface
from flow.orchestrator.handlers import PetriCreateTokenHandler, PetriNotifyPlaceHandler
from flow.orchestrator.handlers import PetriNotifyTransitionHandler

from flow.shell_command.service_interface import ForkShellCommandServiceInterface
from flow.shell_command.handler import ForkShellCommandMessageHandler
from flow.shell_command.executors.fork import ForkExecutor

from flow.petri_net import builder
from flow.shell_command.future_nets import ForkCommandNet

#import time


class TestSystemFork(redistest.RedisTest):
    def setUp(self):
        redistest.RedisTest.setUp(self)

        bindings = {'create_token_x': {'create_token_q':
                                            ['create_token_rk']},
                    'notify_place_x': {'notify_place_q':
                                            ['notify_place_rk']},
                    'notify_transition_x': {'notify_transition_q':
                                            ['notify_transition_rk']},
                    'fork_submit_x': {'fork_submit_q': ['fork_submit_rk']}}
        self.broker = LocalBroker(bindings)

        self.service_interfaces = {
                'orchestrator': OrchestratorServiceInterface(broker=self.broker,
                    create_token_exchange='create_token_x',
                    create_token_routing_key='create_token_rk',
                    notify_place_exchange='notify_place_x',
                    notify_place_routing_key='notify_place_rk',
                    notify_transition_exchange='notify_transition_x',
                    notify_transition_routing_key='notify_transition_rk'),
                'fork': ForkShellCommandServiceInterface(broker=self.broker,
                    exchange='fork_submit_x',
                    submit_routing_key='fork_submit_rk')}

        self.broker.register_handler(
                PetriCreateTokenHandler(redis=self.conn,
                    service_interfaces=self.service_interfaces,
                    queue_name='create_token_q'))
        self.broker.register_handler(
                PetriNotifyPlaceHandler(redis=self.conn,
                    service_interfaces=self.service_interfaces,
                    queue_name='notify_place_q'))
        self.broker.register_handler(
                PetriNotifyTransitionHandler(redis=self.conn,
                    service_interfaces=self.service_interfaces,
                    queue_name='notify_transition_q'))

        fork_executor = ForkExecutor(wrapper=['bash', '-c', ':'],
                default_environment={}, mandatory_environment={})
        self.broker.register_handler(
                ForkShellCommandMessageHandler(
                    executor=fork_executor, queue_name='fork_submit_q',
                    exchange='create_token_x',
                    response_routing_key='create_token_rk'))

    def test_system_fork(self):
        # XXX This test is quite weak, because we rely on the wrapper to talk to
        # the broker even for the fork executor.
        future_net = ForkCommandNet('net name',
                command_line=['non', 'sense', 'command'], stdout='/dev/null')
        future_places, future_transitions = builder.gather_nodes(future_net)

        b = builder.Builder(self.conn)
        net = b.store(future_net, {}, {})

        # XXX OMG, color group
        cg = net.add_color_group(1)

        self.service_interfaces['orchestrator'].create_token(net.key,
                future_places[future_net.start], cg.begin, cg.idx)
        self.broker.listen()

        expected_color_keys = [net.marking_key(
            cg.begin, future_places[future_net.dispatching])]
#        expected_color_keys = ['0:%d' % future_places[future_net.dispatched]]

        self.assertItemsEqual(expected_color_keys, net.color_marking.keys())

if __name__ == "__main__":
    unittest.main()
