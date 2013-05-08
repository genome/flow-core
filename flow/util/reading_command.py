from flow.commands.base import CommandBase
from twisted.internet import defer, reactor
import time
import os
import random

class ReadingCommand(CommandBase):
    injector_modules = []
    @staticmethod
    def annotate_parser(parser):
        parser.add_argument('--file', '-f', type=str)
        parser.add_argument('--time-per-read', '-t', type=float)
        parser.add_argument('--read-size', '-s', type=int)
        parser.add_argument('--repeat', '-r', default=False, action='store_true')
        parser.add_argument('--jump-around', '-j', default=False, action='store_true')

    @defer.inlineCallbacks
    def _execute(self, parsed_arguments):
        filename = parsed_arguments.file
        file_size = os.stat(filename).st_size
        while True:
            with open(filename, 'r', 0) as infile:
                while True:
                    jump_to = -1
                    if parsed_arguments.jump_around:
                        jump_to = random.randint(0, file_size)
                    data = yield self._read_data(infile,
                            parsed_arguments.read_size,
                            parsed_arguments.time_per_read,
                            jump_to=jump_to)
                    if data == '':
                        break
            if not parsed_arguments.repeat:
                break

    def _read_data(self, fh, size, delay, jump_to=-1):
        if jump_to >= 0:
            fh.seek(jump_to)
        data = fh.read(size)

        deferred = defer.Deferred()
        reactor.callLater(delay, deferred.callback, data)
        return deferred
