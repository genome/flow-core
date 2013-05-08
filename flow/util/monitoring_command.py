from flow.commands.base import CommandBase
from twisted.internet import defer
from flow.util.process_monitor import ProcessMonitor
from flow.util.logannotator import LogAnnotator
import os

class MonitoringCommand(CommandBase):
    injector_modules = []
    @staticmethod
    def annotate_parser(parser):
        parser.add_argument('--num-processes', '-n', type=int)
        parser.add_argument('--port', '-p', type=int)
        parser.add_argument('--file', '-f', type=str)
        parser.add_argument('--time-per-read', '-t', type=float)
        parser.add_argument('--read-size', '-s', type=int)
        parser.add_argument('--repeat', '-r', default=False, action='store_true')

    def _execute(self, parsed_arguments):

        process_monitor = ProcessMonitor(os.getpid())
        process_monitor.start(port=parsed_arguments.port)

        deferreds = []
        for i in range(parsed_arguments.num_processes):
            cmdline = ['flow', 'reading-command',
                    '-f', parsed_arguments.file,
                    '-s', str(parsed_arguments.read_size),
                    '-t', str(parsed_arguments.time_per_read)]
            if parsed_arguments.repeat:
                cmdline.append('-r')
            logannotator = LogAnnotator(cmdline)
            deferreds.append(logannotator.start())

        return defer.DeferredList(deferreds)
