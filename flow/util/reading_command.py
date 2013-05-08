from flow.commands.base import CommandBase
from twisted.internet import defer, reactor
import time

class ReadingCommand(CommandBase):
    injector_modules = []
    @staticmethod
    def annotate_parser(parser):
        parser.add_argument('--file', '-f', type=str)
        parser.add_argument('--time-per-read', '-t', type=float)
        parser.add_argument('--read-size', '-s', type=int)
        parser.add_argument('--repeat', '-r', default=False, action='store_true')

    @defer.inlineCallbacks
    def _execute(self, parsed_arguments):
        while True:
            with open(parsed_arguments.file, 'r') as infile:
                while True:
                    data = yield self._read_data(infile,
                            parsed_arguments.read_size,
                            parsed_arguments.time_per_read)
                    if data == '':
                        break
            if not parsed_arguments.repeat:
                break

    def _read_data(self, fh, size, delay):
        deferred = defer.Deferred()
        data = fh.read(size)
        reactor.callLater(delay, deferred.callback, data)
        return deferred
