from flow.commands.base import CommandBase
import logging
import sys
import json
import itertools

LOG = logging.getLogger()


class ConfigureRabbitMQCommand(CommandBase):
    def __init__(self, bindings={}, vhost=''):

        self.bindings_config = bindings
        self.vhost = vhost
        self.rabbit_configuration = None

    @staticmethod
    def annotate_parser(parser):
        parser.add_argument('--output_filename', '-o',
                            default=None, help='Filename to write to. Defaults to STDOUT')

    def __call__(self, parsed_arguments):

        if self.rabbit_configuration is None:
            self.rabbit_configuration = self.empty_rabbit_configuration()

            self.parse_exchanges_queues_bindings()

            self.make_exchange_defs()
            self.make_queue_defs()
            self.make_binding_defs()

        self.dump_config_to_file(parsed_arguments.output_filename)

        return 0

    def parse_exchanges_queues_bindings(self):
        exchanges = self.exchanges = set()
        queues = self.queues = set()
        bindings = self.bindings = set()

        for exchange_name, queue_bindings in self.bindings_config.iteritems():
            exchanges.add(exchange_name)
            for queue_name, topics in queue_bindings.iteritems():
                queues.add(queue_name)
                for topic in topics:
                    bindings.add( (exchange_name, topic, queue_name) )

    def make_exchange_defs(self):

        def new_exchange(name, args):
            return {'name': name,
                    'vhost': self.vhost,
                    'durable': True,
                    'auto_delete': False,
                    'internal': False,
                    'type': 'topic',
                    'arguments': args,
                   }

        result = [ new_exchange('alt', {}) ]
        for exch_name in itertools.chain(['alt'], self.exchanges, ['dead']):
            result.append( new_exchange(exch_name, { 'altername-exchange': 'alt'} ) )
        self.rabbit_configuration['exchanges'] = result

    def make_queue_defs(self):

        def new_queue(name, args):
            return {'name': name,
                    'vhost': self.vhost,
                    'durable': True,
                    'auto_delete': False,
                    'arguments': args,
                    }


        result = [ new_queue( 'missing_routing_key', {} )]
        for queue_name in itertools.chain(['missing_routing_key'], self.queues):
            result.append( new_queue(queue_name, { 'x-dead-letter-exchange': 'dead'} ) )

            if queue_name != 'missing_routing_key':
                result.append( new_queue('dead_' + queue_name, {}) )
        self.rabbit_configuration['queues'] = result


    def make_binding_defs(self):

        def new_binding(source, routing_key, destination):
            return {'vhost': self.vhost,
                    'source': source,
                    'routing_key': routing_key,
                    'destination': destination,
                    'destination_type': 'queue',
                    'arguments': {},
                    }

        result = [ new_binding('alt', '#', 'missing_routing_key') ]
        for name, topic, queue_name in self.bindings:
            result.append( new_binding( name, topic, queue_name ) )
            result.append( new_binding( 'dead', topic, 'dead_' + queue_name) )
        self.rabbit_configuration['bindings'] = result


    def dump_config_to_file(self, output_filename):
        if output_filename:
            of = open(output_filename, 'w')
        else:
            of = sys.stdout

        json.dump(self.rabbit_configuration, of)


    def empty_rabbit_configuration(self):
        return {"rabbit_version":"3.0.1",
            "parameters":[],
            "policies":[],
            "users":[
                {"name":"guest",
                "password_hash":"GvVZRv7FY1mtJZfvN42rcdkLQ/w=",
                "tags":"administrator"}
            ],
            "vhosts":[{"name":"/"}, {"name":"workflow"}, {"name":"testing"}],

            "permissions":[
                {"user":"guest",
                "vhost":"/",
                "configure":".*",
                "write":".*",
                "read":".*"},

                {"user":"guest",
                "vhost":"workflow",
                "configure":".*",
                "write":".*",
                "read":".*"},

                {"user":"guest",
                "vhost":"testing",
                "configure":".*",
                "write":".*",
                "read":".*"}
            ],
        }

