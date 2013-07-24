from flow.commands.base import CommandBase
from flow.configuration.inject.broker import BrokerConfiguration
from flow.configuration.inject.orchestrator import OrchestratorConfiguration
from injector import inject

import flow.interfaces
import logging
import socket


LOG = logging.getLogger(__name__)


@inject(orchestrator=flow.interfaces.IOrchestrator)
class LsfPreExecCommand(CommandBase):
    injector_modules = [
            BrokerConfiguration,
            OrchestratorConfiguration,
    ]

    @staticmethod
    def annotate_parser(parser):
        parser.add_argument('--color', type=int)
        parser.add_argument('--color-group-idx', type=int)
        parser.add_argument('--execute-begin', '-b', type=int)
        parser.add_argument('--net-key', '-n')

    def _execute(self, parsed_arguments):
        LOG.info("Begin LSF pre exec")

        hostname = socket.gethostname()

        deferred = self.orchestrator.create_token(
                net_key=parsed_arguments.net_key,
                place_idx=parsed_arguments.execute_begin,
                color=parsed_arguments.color,
                color_group_idx=parsed_arguments.color_group_idx,
                data={'hostname': hostname})

        return deferred

    def _teardown(self, parsed_arguments):
        LOG.info('End LSF pre exec')
