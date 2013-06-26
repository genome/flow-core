from flow.brokers.local import LocalBroker
from flow.orchestrator.handlers import PetriCreateTokenHandler
from flow.orchestrator.handlers import PetriNotifyPlaceHandler
from flow.orchestrator.handlers import PetriNotifyTransitionHandler
from flow.orchestrator.service_interface import OrchestratorServiceInterface
from flow.petri_net import builder
from flow.shell_command.fork.executor import ForkExecutor
from flow.shell_command.petri_net.future_nets import ForkCommandNet
from flow.shell_command.handler import ForkShellCommandMessageHandler
from flow.shell_command.service_interface import ForkShellCommandServiceInterface
from test_helpers import redistest

import os
import os.path
import tempfile
import unittest

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

        resource_definitions = {}
        fork_executor = ForkExecutor()
        self.broker.register_handler(
                ForkShellCommandMessageHandler(
                    default_environment={}, mandatory_environment={},
                    executor=fork_executor, queue_name='fork_submit_q',
                    service_interfaces=self.service_interfaces,
                    exchange='create_token_x',
                    response_routing_key='create_token_rk',
                    resource_definitions=resource_definitions))

    def test_simple_succeeding_command(self):
        test_data = 'hi'
        output_file = tempfile.NamedTemporaryFile('r')
        future_net = ForkCommandNet('net name',
                command_line=['echo', test_data], stdout=output_file.name,
                stderr='/dev/null')
        future_net.wrap_with_places()
        future_places, future_transitions = builder.gather_nodes(future_net)

        b = builder.Builder(self.conn)
        constants = {
            'user_id': os.getuid(),
            'group_id': os.getgid(),
            'working_directory': os.path.dirname(__file__),
        }
        net = b.store(future_net, {}, constants)

        cg = net.add_color_group(1)

        self.service_interfaces['orchestrator'].create_token(net.key,
                future_places[future_net.start_place], cg.begin, cg.idx)
        self.broker.listen()

        expected_color_keys = [net.marking_key(
            cg.begin, future_places[future_net.success_place])]

        expected_output = '%s\n' % test_data
        self.assertEqual(expected_output, output_file.read())
        self.assertItemsEqual(expected_color_keys, net.color_marking.keys())


    def test_simple_failing_command(self):
        future_net = ForkCommandNet('net name',
                command_line=['ls', '/doesnotexist/fool'], stdout='/dev/null',
                stderr='/dev/null')
        future_net.wrap_with_places()
        future_places, future_transitions = builder.gather_nodes(future_net)


        b = builder.Builder(self.conn)
        constants = {
            'user_id': os.getuid(),
            'group_id': os.getgid(),
        }
        net = b.store(future_net, {}, constants)

        cg = net.add_color_group(1)

        self.service_interfaces['orchestrator'].create_token(net.key,
                future_places[future_net.start_place], cg.begin, cg.idx)
        self.broker.listen()

        expected_color_keys = [net.marking_key(
            cg.begin, future_places[future_net.failure_place])]

        self.assertItemsEqual(expected_color_keys, net.color_marking.keys())


if __name__ == "__main__":
    unittest.main()
