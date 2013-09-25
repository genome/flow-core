#!/usr/bin/env python

from flow.brokers.local import LocalBroker
from flow.orchestrator.handlers import PetriCreateTokenHandler
from flow.orchestrator.handlers import PetriNotifyPlaceHandler
from flow.orchestrator.handlers import PetriNotifyTransitionHandler
from flow.orchestrator.service_interface import OrchestratorServiceInterface
from flow.petri_net import builder
from flow.shell_command.fork.handler import ForkShellCommandMessageHandler
from flow.shell_command.petri_net import actions
from flow.shell_command.petri_net import future_nets
from flow.shell_command.service_interface import ForkShellCommandServiceInterface
from twisted.internet import reactor

import argparse
import os
import redis
import sys
import tempfile


def parse_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument('--redis-socket-path', required=True)
    parser.add_argument('--expect-failure', action='store_true')

    return parser.parse_args()


def command_line(expect_failure):
    if expect_failure:
        return ['ls', '/doesnotexist/fool']
    else:
        return ['echo', 'hi']


def verify_color_marking(net, future_places, color, place_idx):
        expected_color_keys = set([net.marking_key(
            color, future_places[place_idx])])
        actual_color_keys = set(net.color_marking.keys())
        if expected_color_keys != actual_color_keys:
            sys.stderr.write('Color marking mismatch:\n')
            sys.stderr.write('  Expected: %r\n' % expected_color_keys)
            sys.stderr.write('  Actual: %r\n' % actual_color_keys)
            os._exit(1)


def main(redis_socket_path, expect_failure):
    conn = redis.Redis(unix_socket_path=redis_socket_path)

    bindings = {'create_token_x': {'create_token_q':
                                        ['create_token_rk']},
                'notify_place_x': {'notify_place_q':
                                        ['notify_place_rk']},
                'notify_transition_x': {'notify_transition_q':
                                        ['notify_transition_rk']},
                'fork_submit_x': {'fork_submit_q': ['fork_submit_rk']}}
    broker = LocalBroker(bindings=bindings)

    service_interfaces = {
            'orchestrator': OrchestratorServiceInterface(broker=broker,
                create_token_exchange='create_token_x',
                create_token_routing_key='create_token_rk',
                notify_place_exchange='notify_place_x',
                notify_place_routing_key='notify_place_rk',
                notify_transition_exchange='notify_transition_x',
                notify_transition_routing_key='notify_transition_rk'),
            'fork': ForkShellCommandServiceInterface(broker=broker,
                exchange='fork_submit_x',
                submit_routing_key='fork_submit_rk')}

    broker.register_handler(
            PetriCreateTokenHandler(redis=conn,
                service_interfaces=service_interfaces,
                queue_name='create_token_q'))
    broker.register_handler(
            PetriNotifyPlaceHandler(redis=conn,
                service_interfaces=service_interfaces,
                queue_name='notify_place_q'))
    broker.register_handler(
            PetriNotifyTransitionHandler(redis=conn,
                service_interfaces=service_interfaces,
                queue_name='notify_transition_q'))

    resource_type_definitions = {}
    broker.register_handler(
            ForkShellCommandMessageHandler(
                default_environment={}, mandatory_environment={},
                queue_name='fork_submit_q',
                service_interfaces=service_interfaces,
                exchange='create_token_x',
                response_routing_key='create_token_rk'))

    output_file = tempfile.NamedTemporaryFile('r')
    future_net = future_nets.ShellCommandNet('net name',
            dispatch_action_class=actions.ForkDispatchAction,
            command_line=command_line(expect_failure),
            stdout=output_file.name,
            stderr='/dev/null')
    future_net.wrap_with_places()
    future_places, future_transitions = builder.gather_nodes(future_net)

    b = builder.Builder(conn)
    constants = {
        'user_id': os.getuid(),
        'group_id': os.getgid(),
        'umask': 2,
        'working_directory': os.path.abspath(os.path.join(
            os.path.dirname(__file__), '..')),
    }
    net = b.store(future_net, {}, constants)

    cg = net.add_color_group(1)

    service_interfaces['orchestrator'].create_token(net.key,
            future_places[future_net.start_place], cg.begin, cg.idx)
    broker.listen()
    reactor.run()


    if expect_failure:
        verify_color_marking(net, future_places, cg.begin,
                future_net.failure_place)

    else:
        expected_output = 'hi\n'
        actual_output = output_file.read()
        if expected_output != actual_output:
            sys.stderr.write('Output does not match expectation\n')
            sys.stderr.write('  Expected: %s\n' % expected_output)
            sys.stderr.write('  Actual: %s\n' % actual_output)
            os._exit(1)

        verify_color_marking(net, future_places, cg.begin,
                future_net.success_place)


if __name__ == '__main__':
    args = parse_arguments()
    main(redis_socket_path=args.redis_socket_path,
            expect_failure=args.expect_failure)
