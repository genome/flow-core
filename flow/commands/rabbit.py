from flow import exit_codes
from flow.rabbitmq_api import RabbitMQAPI

import injector
import logging
import pprint


LOG = logging.getLogger(__name__)


@injector.inject(api=RabbitMQAPI)
class RabbitCommand(object):
    injector_modules = []

    @classmethod
    def annotate_parser(cls, parser):
        subparsers = parser.add_subparsers()
        queue_parser = subparsers.add_parser('queue',
                help='queue-related commands')
        cls.add_queue_parser(queue_parser)

    @classmethod
    def add_queue_parser(cls, parser):
        parser.add_argument('--regex', default='.*',
                help='regex to match against queue names')

        queue_subparsers = parser.add_subparsers()

        show_parser = queue_subparsers.add_parser('show',
                help='dispaly queue information')
        cls.add_queue_show_parser(show_parser)

        get_parser = queue_subparsers.add_parser('get',
                help='fetch messages from queue')
        cls.add_queue_get_parser(get_parser)

    @classmethod
    def add_queue_show_parser(cls, parser):
        parser.set_defaults(subcommand_func='queue_show')

        parser_property_group = parser.add_mutually_exclusive_group(
                required=True)
        parser_property_group.add_argument('--all', action='store_true',
                required=False, help='show all properties')
        parser_property_group.add_argument('--properties', nargs='*',
                help='properties to show')

    @classmethod
    def add_queue_get_parser(cls, parser):
        parser.set_defaults(subcommand_func='queue_get')

        parser.add_argument('--count', default='1000',
                type=int, help='maximum number of messages to get')
        parser.add_argument('--requeue', type=bool, default=True,
                help='whether to requeue fetched messages')

        parser.add_argument('--full', action='store_true',
                help='whether to return all message data')



    def execute(self, parsed_arguments):
        func = self.subcommand_function(parsed_arguments.subcommand_func)
        result = func(parsed_arguments)

        pprint.pprint(result)

        return exit_codes.EXECUTE_SUCCESS

    def subcommand_function(self, subcommand_string):
        return getattr(self, subcommand_string)


    def queue_show(self, parsed_arguments):
        if parsed_arguments.all:
            return self.api.queue_show_all(parsed_arguments.regex)
        else:
            return self.api.queue_show(parsed_arguments.regex,
                'name', *parsed_arguments.properties)

    def queue_get(self, parsed_arguments):
        return self.api.queue_get(parsed_arguments.regex,
                count=parsed_arguments.count, requeue=parsed_arguments.requeue,
                full=parsed_arguments.full)
