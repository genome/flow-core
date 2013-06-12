from flow.commands.base import CommandBase
from twisted.internet import defer, reactor
from flow.util.process_monitor import ProcessMonitor
from flow.util.logannotator import LogAnnotator
import os

class MonitoringCommand(CommandBase):
    injector_modules = []
    @staticmethod
    def annotate_parser(parser):
        parser.add_argument('--file', '-f', type=str)
        parser.add_argument('--port', '-p', type=int)

    def _execute(self, parsed_arguments):
        process_monitor = ProcessMonitor(os.getpid())
        process_monitor.start(port=parsed_arguments.port)

        cmdline = ['flow', 'reading-command',
                '-f', parsed_arguments.file,
                '-s', str(256),
                '-r',
                '-t', str(0.5)]
        logannotator = LogAnnotator(cmdline)
        long_deferred = logannotator.start()

        cmdline = ['flow', 'reading-command',
                '-f', parsed_arguments.file,
                '-s', str(128),
                '-r',
                '-j',
                '-t', str(0.75)]
        logannotator = LogAnnotator(cmdline)
        logannotator.start()

        self._start_reading(parsed_arguments.file)
        self._start_reading_random()

        return long_deferred

    def _start_reading_random(self):
        cmdline = ['flow', 'random-reading-command',
                '-n', str(5), '-c', str(4)]
        logannotator = LogAnnotator(cmdline)
        this_deferred = logannotator.start()

        this_deferred.addCallback(
                lambda *args: reactor.callLater(1.0, self._start_reading_random))

    def _start_reading(self, filename):
        cmdline = ['flow', 'reading-command',
                '-f', filename,
                '-s', str(128),
                '-t', str(2.0)]
        logannotator = LogAnnotator(cmdline)
        this_deferred = logannotator.start()

        this_deferred.addCallback(
                lambda *args: reactor.callLater(5.0, self._start_reading, filename))
