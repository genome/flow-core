from flow.commands.base import CommandBase
from flow.util.logannotator import LogAnnotator
from twisted.internet import defer, reactor

import os
import random

class ReadingCommand(CommandBase):
    injector_modules = []
    @staticmethod
    def annotate_parser(parser):
        parser.add_argument('--file', '-f', type=str)
        parser.add_argument('--time-per-read', '-t', type=float)
        parser.add_argument('--read-size', '-s', type=int)
        parser.add_argument('--repeat', '-r',
                default=False, action='store_true')
        parser.add_argument('--jump-around', '-j',
                default=False, action='store_true')

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

class RandomReadingCommand(CommandBase):
    injector_modules = []
    @staticmethod
    def annotate_parser(parser):
        parser.add_argument('--num-files', '-n', type=int)
        parser.add_argument('--children', '-c', default=0, type=int)

    def _initialize(self, parsed_arguments):
        filenames = [fn for fn in os.listdir('.') if os.path.isfile(fn)]
        filenames = filenames[:3]

        self.settings = []
        for i in xrange(parsed_arguments.num_files):
            filename = random.choice(filenames)
            settings = {'filename': filename,
                        'read_size': random.choice(
                            [256, 4096, 4096, 4096, 32768, 32768]),
                        'read_time': random.choice(
                            [0.1, 0.25, 0.5, 0.5, 0.5, 0.5, 0.5, 1.0, 4.0]),
                        'read_strategy': random.choice(['normal', 'jump']),
                        'file_size': os.stat(filename).st_size,
                       }
            self.settings.append(settings)

    def _execute(self, parsed_arguments):
        self._initialize(parsed_arguments)

        time_deferred = defer.Deferred()
        reactor.callLater(5, time_deferred.callback, None)
        deferreds = [time_deferred]
        for setting in self.settings:
            fh = open(setting['filename'], 'r', 0)
            deferred = defer.Deferred()
            deferreds.append(deferred)
            self._read_file(fh=fh, deferred=deferred, **setting)

        for i in xrange(parsed_arguments.children):
            cmdline = ['flow', 'random-reading-command',
                    '-n', str(random.choice(range(5))),
                    '-c', str(random.choice([0,0,0,0,0,0,0,1,2]))]
            logannotator = LogAnnotator(cmdline)
            deferred = logannotator.start()
            deferreds.append(deferred)

        return defer.gatherResults(deferreds)

    def _read_file(self, fh, deferred, read_size, file_size, read_time,
            read_strategy, **kwargs):
        if read_strategy == 'jump':
            fh.seek(random.randint(0, file_size))
        data = fh.read(read_size)
        if data == '' or fh.read(1) == '':
            deferred.callback(None)
            fh.close()
        else:
            reactor.callLater(read_time, self._read_file,
                    fh=fh,
                    deferred=deferred,
                    read_size=read_size,
                    file_size=file_size,
                    read_time=read_time,
                    read_strategy=read_strategy)
