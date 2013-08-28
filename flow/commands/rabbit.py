from flow import exit_codes
from flow.rabbit.api import RabbitMQAPI
from flow.rabbit.filter.factory import FilterFactory
from flow.rabbit.reporter.factory import ReporterFactory

import injector
import logging


LOG = logging.getLogger(__name__)


@injector.inject(api=RabbitMQAPI, filter_factory=FilterFactory,
        reporter_factory=ReporterFactory)
class RabbitCommand(object):
    injector_modules = []

    @classmethod
    def annotate_parser(cls, parser):
        subparsers = parser.add_subparsers()
        cls.add_queue_parser(subparsers)
        cls.add_necro_parser(subparsers)

    @classmethod
    def add_queue_parser(cls, parent_subparsers):
        parser = parent_subparsers.add_parser('queue',
                help='queue-related commands')
        parser.add_argument('--regex', default='.*',
                help='regex to match against queue names')
        parser.add_argument('--report_type')

        queue_subparsers = parser.add_subparsers()

        show_parser = queue_subparsers.add_parser('show',
                help='dispaly queue information')
        cls.add_queue_show_parser(show_parser)

        get_parser = queue_subparsers.add_parser('get',
                help='fetch messages from queue')
        get_parser.set_defaults(subcommand_func='queue_get')
        cls.add_queue_get_parser(get_parser)


    @classmethod
    def add_queue_show_parser(cls, parser):
        parser.set_defaults(subcommand_func='queue_show')

        filter_group = parser.add_mutually_exclusive_group()
        filter_group.add_argument('--select_property_names', nargs='+')
        filter_group.add_argument('--blocked_property_names',
                default=['deliveries'], nargs='+')

        parser.set_defaults(report_type='csv')


    @classmethod
    def add_queue_get_parser(cls, parser):

        parser.add_argument('--count', default='1000',
                type=int, help='maximum number of messages to get')
        parser.add_argument('--requeue', type=bool, default=True,
                help='whether to requeue fetched messages')

        parser.add_argument('--full', action='store_true',
                help='whether to return all message data')

    @classmethod
    def add_necro_parser(cls, parent_subparsers):
        parser = parent_subparsers.add_parser('necro',
                help='necromancy-related commands')
        parser.add_argument('--report_type')
        necro_subparsers = parser.add_subparsers()

        cls.add_necro_revive_parser(necro_subparsers)
        cls.add_necro_kill_parser(necro_subparsers)

    @classmethod
    def add_necro_revive_parser(cls, necro_subparsers):
        parser = necro_subparsers.add_parser('revive',
                help='requeue dead messages')
        parser.set_defaults(subcommand_func='necro_revive')
        parser.add_argument('--regex', default='.*',
                help='regex to match against queue names')

        cls.add_queue_get_parser(parser)

    @classmethod
    def add_necro_kill_parser(cls, necro_subparsers):
        parser = necro_subparsers.add_parser('kill',
                help='requeue dead messages')
        parser.set_defaults(subcommand_func='necro_kill')
        parser.add_argument('--regex', default='.*',
                help='regex to match against queue names')

        parser.add_argument('--count', default=1000)


    def execute(self, parsed_arguments):
        func = self.subcommand_function(parsed_arguments.subcommand_func)
        result = func(parsed_arguments)

        reporter = self.reporter_factory.create(parsed_arguments)
        reporter(result)

        return exit_codes.EXECUTE_SUCCESS

    def subcommand_function(self, subcommand_string):
        return getattr(self, subcommand_string)


    def queue_show(self, parsed_arguments):
        queue_filter = self.filter_factory.create(parsed_arguments)
        return self.api.queue_show(parsed_arguments.regex, queue_filter)

    def queue_get(self, parsed_arguments):
        return self.api.queue_get(parsed_arguments.regex,
                count=parsed_arguments.count, requeue=parsed_arguments.requeue,
                full=parsed_arguments.full)

    def necro_kill(self, parsed_arguments):
        return self.api.queue_get(parsed_arguments.regex,
                count=parsed_arguments.count, requeue=False, full=True)


    def necro_revive(self, parsed_arguments):
        parsed_arguments.report_type = None

        all_dead_messages = self.api.queue_get(
                'dead_.*%s' % parsed_arguments.regex,
                count=parsed_arguments.count, requeue=parsed_arguments.requeue,
                full=True)

        for dead_queue, dead_messages in all_dead_messages.iteritems():
            for dead_message in dead_messages:
                queue_name = self._revive_queue_name(dead_queue, dead_message)
                LOG.debug("Resurecting message to queue: %s, msg: %s", queue_name, dead_message['payload'])
                self.api.publish_to_queue(queue_name,
                        payload=dead_message['payload'],
                        payload_encoding=dead_message['payload_encoding'],
                        message_properties=dead_message['properties'])

        return all_dead_messages

    def _revive_queue_name(self, dead_queue, dead_message):
        return dead_queue[5:]
