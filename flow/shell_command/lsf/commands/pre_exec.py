from flow.commands.base import CommandBase
from flow import exit_codes
from flow.configuration.inject.broker import BrokerConfiguration
from flow.configuration.inject.orchestrator import OrchestratorConfiguration
from flow.configuration.settings.injector import setting
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
        parser.add_argument('--execute-begin', '-b')
        parser.add_argument('--net-key', '-n')

    def _execute(self, parsed_arguments):
        LOG.info("Begin LSF pre exec")

        deferred = self.orchestrator.create_token(
                net_key=parsed_arguments.net_key,
                place_idx=parsed_arguments.execute_begin,
                color=parsed_arguments.color,
                color_group_idx=parsed_arguments.color_group_idx)

        return deferred

    def _teardown(self, parsed_arguments):
        LOG.info('End LSF pre exec')
