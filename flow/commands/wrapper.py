from flow.commands.base import CommandBase
from flow.petri.safenet import Token, SetTokenMessage
import subprocess

import logging

LOG = logging.getLogger()


class WrapperCommand(CommandBase):
    default_logging_mode = 'debug'

    def __init__(self, broker=None, storage=None, routing_key=None):
        self.broker = broker
        self.storage = storage
        self.routing_key = routing_key


    @staticmethod
    def annotate_parser(parser):
        parser.add_argument('--net-key', '-n')
        parser.add_argument('--running-place-id', '-r', type=int)
        parser.add_argument('--success-place-id', '-s', type=int)
        parser.add_argument('--failure-place-id', '-f', type=int)

        parser.add_argument('command_line', nargs='+')

    def __call__(self, parsed_arguments):
        self.send_token(net_key=parsed_arguments.net_key,
                place_idx=parsed_arguments.running_place_id)

        rv = subprocess.call(parsed_arguments.command_line)

        if rv == 0:
            self.send_token(net_key=parsed_arguments.net_key,
                    place_idx=parsed_arguments.success_place_id)
        else:
            self.send_token(net_key=parsed_arguments.net_key,
                    place_idx=parsed_arguments.failure_place_id)

        return rv

    def send_token(self, net_key=None, place_idx=None):
        self.broker.connect()
        token = Token.create(self.storage)

        message = SetTokenMessage(net_key=net_key, place_idx=place_idx,
                token_key=token.key)
        self.broker.publish(routing_key=self.routing_key, message=message)
        self.broker.disconnect()
