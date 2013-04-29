from flow.commands.token_sender import TokenSenderCommand
from twisted.internet import defer
from flow.configuration.settings.injector import setting
from flow.configuration.inject.redis_conf import RedisConfiguration
from flow.configuration.inject.broker import BrokerConfiguration
from injector import inject
from tempfile import NamedTemporaryFile
from flow.util.logannotator import LogAnnotator

import flow.interfaces
import flow.redisom as rom
import json
import logging
import os
import platform


LOG = logging.getLogger(__name__)

@inject(storage=flow.interfaces.IStorage)
class WrapperCommand(TokenSenderCommand):
    injector_modules = [
            RedisConfiguration,
            BrokerConfiguration,
    ]

    @staticmethod
    def annotate_parser(parser):
        parser.add_argument('--net-key', '-n')
        parser.add_argument('--running-place-id', '-r', type=int)
        parser.add_argument('--success-place-id', '-s', type=int)
        parser.add_argument('--failure-place-id', '-f', default=None, type=int)
        parser.add_argument('--with-inputs', default=None, type=str)
        parser.add_argument('--with-outputs', default=False,
                action="store_true")
        parser.add_argument('--token-color', default=None)

        parser.add_argument('command_line', nargs='+')

    def _execute(self, parsed_arguments):
        execute_deferred = defer.Deferred()
        self.__execute(parsed_arguments, execute_deferred)
        return execute_deferred

    @defer.inlineCallbacks
    def __execute(self, parsed_arguments, execute_deferred):
        deferreds = []
        deferred = self.send_token(net_key=parsed_arguments.net_key,
                place_idx=parsed_arguments.running_place_id,
                data={"pid": str(os.getpid())},
                color=parsed_arguments.token_color)
        deferreds.append(deferred)

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

                    LOG.debug("Fetched inputs from key %s",
                            parsed_arguments.with_inputs)
                    LOG.debug("Input values: %r", inputs)

                    json.dump(inputs, inputs_file)
                    inputs_file.flush()
                    cmdline += ["--inputs-file", inputs_file.name]
                else:
                    LOG.debug("No inputs for command")

                if parsed_arguments.with_outputs:
                    cmdline += ["--outputs-file", outputs_file.name]

                LOG.info("On host %s: executing %s", platform.node(),
                        " ".join(cmdline))

                logannotator = LogAnnotator(cmdline)
                self.exit_code = yield logannotator.start()

                if self.exit_code == 0:
                    outputs = None
                    if parsed_arguments.with_outputs:
                        outputs = json.load(outputs_file)

                    deferred = self.send_token(net_key=parsed_arguments.net_key,
                            place_idx=parsed_arguments.success_place_id,
                            data={"exit_code": 0, "outputs": outputs},
                            color=parsed_arguments.token_color)
                    deferreds.append(deferred)
                else:
                    LOG.info("Failed to execute command '%s'.",
                            " ".join(cmdline))
                    if parsed_arguments.failure_place_id is not None:
                        deferred = self.send_token(
                                net_key=parsed_arguments.net_key,
                                place_idx=parsed_arguments.failure_place_id,
                                data={"exit_code": self.exit_code},
                                color=parsed_arguments.token_color)
                        deferreds.append(deferred)

        dlist = defer.DeferredList(deferreds)
        dlist.chainDeferred(execute_deferred)
