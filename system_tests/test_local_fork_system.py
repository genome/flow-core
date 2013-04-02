import unittest
from test_helpers import redistest

from flow.brokers.local import LocalBroker
from flow.orchestrator.service_interface import OrchestratorServiceInterface
from flow.orchestrator.handlers import PetriSetTokenHandler
from flow.orchestrator.handlers import PetriNotifyTransitionHandler

from flow.command_runner.service_interface import CommandLineServiceInterface
from flow.command_runner.handler import CommandLineSubmitMessageHandler
from flow.command_runner.executors.local import SubprocessExecutor

from flow.petri import safenet
from flow.petri.netbuilder import NetBuilder
from flow.command_runner.executors import nets


class TestSystemFork(redistest.RedisTest):
    def setUp(self):
        redistest.RedisTest.setUp(self)

        bindings = {'set_token_x': {'set_token_q': ['set_token_rk']},
                    'notify_transition_x': {'notify_transition_q':
                                            ['notify_transition_rk']},
                    'fork_submit_x': {'fork_submit_q': ['fork_submit_rk']}}
        self.broker = LocalBroker(bindings)

        self.service_interfaces = {
                'orchestrator': OrchestratorServiceInterface(broker=self.broker,
                    set_token_exchange='set_token_x',
                    set_token_routing_key='set_token_rk',
                    notify_transition_exchange='notify_transition_x',
                    notify_transition_routing_key='notify_transition_rk'),
                'fork': CommandLineServiceInterface(broker=self.broker,
                    exchange='fork_submit_x',
                    submit_routing_key='fork_submit_rk')}

        self.broker.register_handler(
                PetriSetTokenHandler(redis=self.conn,
                    service_interfaces=self.service_interfaces,
                    queue_name='set_token_q'))
        self.broker.register_handler(
                PetriNotifyTransitionHandler(redis=self.conn,
                    service_interfaces=self.service_interfaces,
                    queue_name='notify_transition_q'))

        fork_executor = SubprocessExecutor(wrapper=['bash', '-c', ':'])
        self.broker.register_handler(
                CommandLineSubmitMessageHandler(
                    broker=self.broker, storage=self.conn,
                    executor=fork_executor, queue_name='fork_submit_q',
                    exchange='set_token_x', routing_key='set_token_rk'))

    def test_system_fork(self):
        # XXX This test is quite weak, because we rely on the wrapper to talk to
        # the broker even for the fork executor.
        builder = NetBuilder()
        building_net = nets.LocalCommandNet(builder, 'net name',
                nets.LocalDispatchAction,
                action_args={'command_line': ['non', 'sense', 'command'],
                    'stdout': '/dev/null'})

        net = builder.store(self.conn)

        token = safenet.Token.create(self.conn)
        self.service_interfaces['orchestrator'].set_token(net.key,
                0, token_key=token.key)
        self.broker.listen()

        # XXX This is the marking for dispatched, not success/failure
        self.assertEqual(['3'], net.marking.keys())

if __name__ == "__main__":
    unittest.main()
