from tempfile import NamedTemporaryFile
import flow.redisom as rom
import json
import subprocess
import os
import platform
import logging

from flow.commands.token_sender import TokenSenderCommand

LOG = logging.getLogger(__name__)


class WrapperCommand(TokenSenderCommand):
    @staticmethod
    def annotate_parser(parser):
        parser.add_argument('--net-key', '-n')
        parser.add_argument('--running-place-id', '-r', type=int)
        parser.add_argument('--success-place-id', '-s', type=int)
        parser.add_argument('--failure-place-id', '-f', default=None, type=int)
        parser.add_argument('--with-inputs', default=None, type=str)
        parser.add_argument('--with-outputs', default=False,
                action="store_true")

        parser.add_argument('command_line', nargs='+')

    def __call__(self, parsed_arguments):
        self.send_token(net_key=parsed_arguments.net_key,
                place_idx=parsed_arguments.running_place_id,
                data={"pid": str(os.getpid())})

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

                    LOG.debug("Fetched inputs from key %s" %
                            parsed_arguments.with_inputs)
                    LOG.debug("Input values: %r" % inputs)

                    json.dump(inputs, inputs_file)
                    inputs_file.flush()
                    cmdline += ["--inputs-file", inputs_file.name]
                else:
                    LOG.debug("No inputs for command")

                if parsed_arguments.with_outputs:
                    cmdline += ["--outputs-file", outputs_file.name]

                LOG.info("On host %s: executing %s", platform.node(),
                        " ".join(cmdline))

                try:
                    subprocess.check_call(cmdline)

                    outputs = None
                    if parsed_arguments.with_outputs:
                        outputs = json.load(outputs_file)

                    self.send_token(net_key=parsed_arguments.net_key,
                            place_idx=parsed_arguments.success_place_id,
                            data={"exit_code": 0, "outputs": outputs})

                    rv = 0
                except subprocess.CalledProcessError as e:
                    LOG.info("Failed to execute command '%s': %s",
                            " ".join(cmdline), str(e))
                    if parsed_arguments.failure_place_id is not None:
                        self.send_token(net_key=parsed_arguments.net_key,
                                place_idx=parsed_arguments.failure_place_id,
                                data={"exit_code": e.returncode})

                    rv = e.returncode

        return rv
