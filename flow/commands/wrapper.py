from flow.commands.base import CommandBase
from flow.petri.safenet import Token, SetTokenMessage
from tempfile import NamedTemporaryFile
import flow.redisom as rom
import json
import subprocess

import logging

LOG = logging.getLogger(__name__)


class WrapperCommand(CommandBase):
    default_logging_mode = 'debug'

    def __init__(self, broker=None, storage=None, routing_key=None, exchange=None):
        self.broker = broker
        self.storage = storage
        self.routing_key = routing_key
        self.exchange = exchange


    @staticmethod
    def annotate_parser(parser):
        parser.add_argument('--net-key', '-n')
        parser.add_argument('--running-place-id', '-r', type=int)
        parser.add_argument('--success-place-id', '-s', type=int)
        parser.add_argument('--failure-place-id', '-f', type=int)
        parser.add_argument('--with-inputs', default=None, type=str)
        parser.add_argument('--with-outputs', default=False,
                action="store_true")

        parser.add_argument('command_line', nargs='+')

    def __call__(self, parsed_arguments):
        self.send_token(net_key=parsed_arguments.net_key,
                place_idx=parsed_arguments.running_place_id)

        rv = 1
        cmdline = parsed_arguments.command_line

        with NamedTemporaryFile() as inputs_file:
            with NamedTemporaryFile() as outputs_file:
                if parsed_arguments.with_inputs:
                    inputs_hash = rom.Hash(
                            connection=self.storage,
                            key=parsed_arguments.with_inputs,
                            value_decoder=rom.json_dec,
                            value_encoder=rom.json_enc)

                    inputs = inputs_hash.value

                    LOG.debug("Fetched inputs from key %s" % parsed_arguments.with_inputs)
                    LOG.debug("Input values: %r" % inputs)

                    json.dump(inputs, inputs_file)
                    inputs_file.flush()
                    cmdline += ["--inputs-file", inputs_file.name]

                if parsed_arguments.with_outputs:
                    cmdline += ["--outputs-file", outputs_file.name]

                rv = subprocess.call(cmdline)

                if rv == 0:
                    outputs = None
                    if parsed_arguments.with_outputs:
                        outputs = json.load(outputs_file)

                    self.send_token(net_key=parsed_arguments.net_key,
                            place_idx=parsed_arguments.success_place_id,
                            data=outputs)
                else:
                    self.send_token(net_key=parsed_arguments.net_key,
                            place_idx=parsed_arguments.failure_place_id)

        return rv

    def send_token(self, net_key=None, place_idx=None, data=None):
        self.broker.connect()
        token = Token.create(self.storage, data=data, data_type="output")

        message = SetTokenMessage(net_key=net_key, place_idx=place_idx,
                token_key=token.key)
        self.broker.publish(exchange_name=self.exchange, routing_key=self.routing_key,
                message=message)
        self.broker.disconnect()
