from flow.commands.base import CommandBase

class BenchmarkCommand(CommandBase):
    @staticmethod
    def annotate_parser(parser):
        parser.add_argument('--bar', default=False, action='store_true')

    def __call__(self, parsed_arguments):
        pass
