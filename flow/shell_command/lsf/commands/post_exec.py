from flow import exit_codes
from flow.commands.base import CommandBase
from flow.configuration.inject.broker import BrokerConfiguration
from flow.configuration.inject.orchestrator import OrchestratorConfiguration
from flow.configuration.settings.injector import setting
from flow.util.exit import exit
from injector import inject

import flow.interfaces
import logging
import os


LOG = logging.getLogger(__name__)


@inject(orchestrator=flow.interfaces.IOrchestrator)
class LsfPostExecCommand(CommandBase):
    injector_modules = [
            BrokerConfiguration,
            OrchestratorConfiguration,
    ]

    @staticmethod
    def annotate_parser(parser):
        parser.add_argument('--color')
        parser.add_argument('--color-group-idx')
        parser.add_argument('--execute-failure', '-f')
        parser.add_argument('--execute-success', '-s')
        parser.add_argument('--net-key', '-n')

    def _execute(self, parsed_arguments):
        LOG.info("Begin LSF post exec")

        info = os.environ.get('LSB_JOBEXIT_INFO', None)
        stat = os.environ.get('LSB_JOBEXIT_STAT', None)
        if stat is None:
            LOG.critical("LSB_JOBEXIT_STAT environment variable wasn't "
                    "set... exiting!")
            exit(exit_codes.EXECUTE_ERROR)
        else:
            stat = int(stat)

        # we don't currently do migrating/checkpointing/requing so we're not
        # going to check for those posibilities.  Instead we will assume that
        # the job has failed.
        if info is not None or stat != 0:
            exit_code = stat >> 8
            signal = stat & 255

            LOG.debug('Job exitted with code (%s) and signal (%s)',
                    exit_code, signal)
            deferred = self.orchestrator.create_token(
                    net_key=parsed_arguments.net_key,
                    place_idx=parsed_arguments.execute_failure,
                    color=parsed_arguments.color,
                    color_group_idx=parsed_arguments.color_group_idx)

        else:
            LOG.debug("Process exited normally")
            deferred = self.orchestrator.create_token(
                    net_key=parsed_arguments.net_key,
                    place_idx=parsed_arguments.execute_success,
                    color=parsed_arguments.color,
                    color_group_idx=parsed_arguments.color_group_idx)

        return deferred

    def _teardown(self, parsed_arguments):
        LOG.info('End LSF post exec')
