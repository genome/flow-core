#!/usr/bin/env python

import unittest
import tempfile
import flow.commands.base
import json


class TestConfigureRabbitMQ(unittest.TestCase):
    # FIXME - this is really ugly
    # Ths expected output is based on running the old configre-rabbitmq
    # command from the flow-core repo as of 748111 on the config file in
    # the flow-site repo as of cbaf66

    def setUp(self):
        temp_config_file = tempfile.NamedTemporaryFile()
        temp_config_file.write( configuration_file_data() )
        temp_config_file.flush()

        self.temp_config_file = temp_config_file


    def test_create_configuration(self):
        temp_output_file = tempfile.NamedTemporaryFile()

        rv = flow.commands.base.run(['--config', self.temp_config_file.name,
            'configure_rabbitmq', '--output_file', temp_output_file.name])
        self.assertEqual(rv, 0, 'running command returned 0 exit code')

        read_output_file = open(temp_output_file.name, 'r')
        output_data = json.loads( read_output_file.read() )
        expected = json.loads( expected_output() )

        self.assertEqual(output_data, expected, 'output data matches expected')


def configuration_file_data():
    return """brokers:
    - &strategic_asynchronous_broker
      factory_name: asynchronous_amqp_broker
      amqp_url: 'amqp://guest:guest@vmpool82:5672/workflow'
      prefetch_count: 10
      acking_strategy:
          factory_name: publisher_confirm_acking

    - &blocking_broker
      factory_name: blocking_broker
      amqp_url: 'amqp://guest:guest@vmpool82:5672/workflow'


storage_components:
    - &vm_redis
      factory_name: redis_storage_singleton
      host: 'vmpool83'

vhost: &vhost
    workflow

bindings:
    &bindings
    workflow:
        workflow_success:
            - genome.shortcut.success
            - genome.execute.success

        workflow_failure:
            - genome.shortcut.failure
            - genome.execute.failure

        flow_execute_node:
            - flow.node.execute

        flow_status_request:
            - flow.status.request

        subprocess_submit:
            - genome.shortcut.submit

        lsf_submit:
            - genome.execute.submit

        flow_fire_transition:
            - flow.transition.fire

        flow_add_tokens:
            - flow.place.add_tokens

        petri_notify_place:
            - petri.place.notify

        # Used in redis lua script prototype
        petri_set_token:
            - petri.place.set_token

        petri_notify_transition:
            - petri.transition.notify

commands:
    orchestrator:
        broker:
            <<: *strategic_asynchronous_broker
            exchange_name: workflow

        storage: *vm_redis

        service_interfaces:
            factory_name: dictionary_factory
            orchestrator:
                factory_name: orchestrator_service_interface
                execute_node_routing_key: flow.node.execute

            genome_shortcut:
                factory_name: shell_command_service_interface
                submit_routing_key: genome.shortcut.submit
                success_routing_key: genome.shortcut.success
                failure_routing_key: genome.shortcut.failure
                error_routing_key: genome.shortcut.error
                wrapper: []

            genome_execute:
                factory_name: shell_command_service_interface
                submit_routing_key: genome.execute.submit
                success_routing_key: genome.execute.success
                failure_routing_key: genome.execute.failure
                error_routing_key: genome.execute.error
                wrapper: []

        handlers:
            - factory_name: execute_node_handler
              queue_name: flow_execute_node

            - factory_name: node_status_request_handler
              queue_name: flow_status_request

            - factory_name: method_descriptor_handler
              queue_name: workflow_success
              callback_name: on_success

            - factory_name: method_descriptor_handler
              queue_name: workflow_failure
              callback_name: on_failure

    local_command_line_service:
        broker:
            <<: *strategic_asynchronous_broker
            exchange_name: workflow

        storage: null
        handlers:
            - queue_name: subprocess_submit
              factory_name: command_line_submit_handler
              executor:
                  factory_name: command_line_local_executor
                  mandatory_environment: {
                      MASKED_ENVIRONMENT_VARIABLE: 'Bad panda!  Do not look here!'
                  }

    lsf_command_line_service:
        broker:
            <<: *strategic_asynchronous_broker
            exchange_name: workflow

        storage: null
        handlers:
            # XXX Maybe the CLSMH constructor should construct its own executor?
            - queue_name: lsf_submit
              factory_name: command_line_submit_handler
              executor:
                  factory_name: command_line_lsf_executor
                  default_queue: long
                  default_environment: {
                      LSF_SERVERDIR: '/usr/local/lsf/8.0/linux2.6-glibc2.3-x86_64/etc',
                      LSF_LIBDIR: '/usr/local/lsf/8.0/linux2.6-glibc2.3-x86_64/lib',
                      LSF_BINDIR: '/usr/local/lsf/8.0/linux2.6-glibc2.3-x86_64/bin',
                      LSF_ENVDIR: '/usr/local/lsf/conf'
                  }
                  mandatory_environment: {
                      MASKED_ENVIRONMENT_VARIABLE: 'Bad panda!  Do not look here!'
                  }

    status:
        broker:
            <<: *blocking_broker
            exchange_name: workflow

        request_routing_key: 'flow.status.request'
        responder_exchange: 'workflow'
        response_routing_key_template: 'flow.status.response.%s'
        queue_template: 'flow_status_response_%s'

    benchmark:
        broker:
            <<: *blocking_broker
            exchange_name: workflow
        execute_node_routing_key: flow.node.execute
        storage: *vm_redis
        status_getter:
            factory_name: status_command
            request_routing_key: 'flow.status.request'
            responder_exchange: 'workflow'
            response_routing_key_template: 'flow.status.response.%s'
            queue_template: 'flow_status_response_%s'

    configure_rabbitmq:
        bindings: *bindings
        vhost: *vhost
        server_config:
            rabbit_version: '3.0.1'
            parameters: []
            policies: []
            users:
                - name: "guest"
                  password_hash: "abc123zyx987"
                  tags: "administrator"
            vhosts:
                - name: "/"
                - name: "workflow"
                - name: "testing"
            permissions:
                - user: "guest"
                  vhost: "/"
                  configure: ".*"
                  write: ".*"
                  read: ".*"
                - user: "guest"
                  vhost: "workflow"
                  configure: ".*"
                  write: ".*"
                  read: ".*"
                - user: "guest"
                  vhost: "testing"
                  configure: ".*"
                  write: ".*"
                  read: ".*"


logging_configurations:
    default:
        formatters:
            color:
                '()': flow.util.log_formatter.ColorFormatter
                fmt: '%(levelname)-23s %(asctime)s %(name)-60s %(funcName)-50s %(lineno)5d: %(message)s'

        root:
            level: INFO
            handlers: ['console']

        loggers:
            pika:
                level: INFO
            flow:
                level: INFO

        handlers:
            console:
                class: logging.StreamHandler
                formatter: color

        version: 1
        disable_existing_loggers: true

    debug:
        formatters:
            color:
                '()': flow.util.log_formatter.ColorFormatter
                fmt: '%(levelname)-23s %(asctime)s %(name)-60s %(funcName)-50s %(lineno)5d: %(message)s'

        root:
            level: DEBUG
            handlers: ['console']

        loggers:
            pika:
                level: INFO
            flow:
                level: DEBUG

        handlers:
            console:
                class: logging.StreamHandler
                formatter: color

        version: 1
        disable_existing_loggers: true

    'no-color':
        formatters:
            plain:
                format: '%(levelname)-8s %(asctime)s %(name)-45s %(funcName)-35s %(lineno)5d: %(message)s'

        root:
            level: INFO
            handlers: ['console']

        loggers:
            pika:
                level: INFO
            flow:
                level: INFO

        handlers:
            console:
                class: logging.StreamHandler
                formatter: plain

        version: 1
        disable_existing_loggers: true

    silent:
        version: 1

# XXX This is commented out because of an apparent bug with pika.
# pika.adapters.base_connection has no logging handler -> exception
#        disable_existing_loggers: true"""


def expected_output():
    return """{"users": [{"tags": "administrator", "name": "guest", "password_hash": "abc123zyx987"}], "parameters": [], "vhosts": [{"name": "/"}, {"name": "workflow"}, {"name": "testing"}], "queues": [{"vhost": "workflow", "durable": true, "auto_delete": false, "name": "missing_routing_key", "arguments": {}}, {"vhost": "workflow", "durable": true, "auto_delete": false, "name": "lsf_submit", "arguments": {"x-dead-letter-exchange": "dead"}}, {"vhost": "workflow", "durable": true, "auto_delete": false, "name": "dead_lsf_submit", "arguments": {}}, {"vhost": "workflow", "durable": true, "auto_delete": false, "name": "petri_notify_transition", "arguments": {"x-dead-letter-exchange": "dead"}}, {"vhost": "workflow", "durable": true, "auto_delete": false, "name": "dead_petri_notify_transition", "arguments": {}}, {"vhost": "workflow", "durable": true, "auto_delete": false, "name": "flow_status_request", "arguments": {"x-dead-letter-exchange": "dead"}}, {"vhost": "workflow", "durable": true, "auto_delete": false, "name": "dead_flow_status_request", "arguments": {}}, {"vhost": "workflow", "durable": true, "auto_delete": false, "name": "subprocess_submit", "arguments": {"x-dead-letter-exchange": "dead"}}, {"vhost": "workflow", "durable": true, "auto_delete": false, "name": "dead_subprocess_submit", "arguments": {}}, {"vhost": "workflow", "durable": true, "auto_delete": false, "name": "flow_execute_node", "arguments": {"x-dead-letter-exchange": "dead"}}, {"vhost": "workflow", "durable": true, "auto_delete": false, "name": "dead_flow_execute_node", "arguments": {}}, {"vhost": "workflow", "durable": true, "auto_delete": false, "name": "flow_fire_transition", "arguments": {"x-dead-letter-exchange": "dead"}}, {"vhost": "workflow", "durable": true, "auto_delete": false, "name": "dead_flow_fire_transition", "arguments": {}}, {"vhost": "workflow", "durable": true, "auto_delete": false, "name": "petri_notify_place", "arguments": {"x-dead-letter-exchange": "dead"}}, {"vhost": "workflow", "durable": true, "auto_delete": false, "name": "dead_petri_notify_place", "arguments": {}}, {"vhost": "workflow", "durable": true, "auto_delete": false, "name": "workflow_failure", "arguments": {"x-dead-letter-exchange": "dead"}}, {"vhost": "workflow", "durable": true, "auto_delete": false, "name": "dead_workflow_failure", "arguments": {}}, {"vhost": "workflow", "durable": true, "auto_delete": false, "name": "flow_add_tokens", "arguments": {"x-dead-letter-exchange": "dead"}}, {"vhost": "workflow", "durable": true, "auto_delete": false, "name": "dead_flow_add_tokens", "arguments": {}}, {"vhost": "workflow", "durable": true, "auto_delete": false, "name": "workflow_success", "arguments": {"x-dead-letter-exchange": "dead"}}, {"vhost": "workflow", "durable": true, "auto_delete": false, "name": "dead_workflow_success", "arguments": {}}, {"vhost": "workflow", "durable": true, "auto_delete": false, "name": "petri_set_token", "arguments": {"x-dead-letter-exchange": "dead"}}, {"vhost": "workflow", "durable": true, "auto_delete": false, "name": "dead_petri_set_token", "arguments": {}}], "policies": [], "exchanges": [{"vhost": "workflow", "internal": false, "arguments": {}, "durable": true, "type": "topic", "auto_delete": false, "name": "alt"}, {"vhost": "workflow", "internal": false, "arguments": {"alternate-exchange": "alt"}, "durable": true, "type": "topic", "auto_delete": false, "name": "workflow"}, {"vhost": "workflow", "internal": false, "arguments": {"alternate-exchange": "alt"}, "durable": true, "type": "topic", "auto_delete": false, "name": "dead"}], "bindings": [{"vhost": "workflow", "destination": "missing_routing_key", "routing_key": "#", "source": "alt", "arguments": {}, "destination_type": "queue"}, {"vhost": "workflow", "destination": "flow_status_request", "routing_key": "flow.status.request", "source": "workflow", "arguments": {}, "destination_type": "queue"}, {"vhost": "workflow", "destination": "dead_flow_status_request", "routing_key": "flow.status.request", "source": "dead", "arguments": {}, "destination_type": "queue"}, {"vhost": "workflow", "destination": "workflow_failure", "routing_key": "genome.shortcut.failure", "source": "workflow", "arguments": {}, "destination_type": "queue"}, {"vhost": "workflow", "destination": "dead_workflow_failure", "routing_key": "genome.shortcut.failure", "source": "dead", "arguments": {}, "destination_type": "queue"}, {"vhost": "workflow", "destination": "workflow_success", "routing_key": "genome.shortcut.success", "source": "workflow", "arguments": {}, "destination_type": "queue"}, {"vhost": "workflow", "destination": "dead_workflow_success", "routing_key": "genome.shortcut.success", "source": "dead", "arguments": {}, "destination_type": "queue"}, {"vhost": "workflow", "destination": "petri_notify_place", "routing_key": "petri.place.notify", "source": "workflow", "arguments": {}, "destination_type": "queue"}, {"vhost": "workflow", "destination": "dead_petri_notify_place", "routing_key": "petri.place.notify", "source": "dead", "arguments": {}, "destination_type": "queue"}, {"vhost": "workflow", "destination": "petri_set_token", "routing_key": "petri.place.set_token", "source": "workflow", "arguments": {}, "destination_type": "queue"}, {"vhost": "workflow", "destination": "dead_petri_set_token", "routing_key": "petri.place.set_token", "source": "dead", "arguments": {}, "destination_type": "queue"}, {"vhost": "workflow", "destination": "workflow_success", "routing_key": "genome.execute.success", "source": "workflow", "arguments": {}, "destination_type": "queue"}, {"vhost": "workflow", "destination": "dead_workflow_success", "routing_key": "genome.execute.success", "source": "dead", "arguments": {}, "destination_type": "queue"}, {"vhost": "workflow", "destination": "workflow_failure", "routing_key": "genome.execute.failure", "source": "workflow", "arguments": {}, "destination_type": "queue"}, {"vhost": "workflow", "destination": "dead_workflow_failure", "routing_key": "genome.execute.failure", "source": "dead", "arguments": {}, "destination_type": "queue"}, {"vhost": "workflow", "destination": "subprocess_submit", "routing_key": "genome.shortcut.submit", "source": "workflow", "arguments": {}, "destination_type": "queue"}, {"vhost": "workflow", "destination": "dead_subprocess_submit", "routing_key": "genome.shortcut.submit", "source": "dead", "arguments": {}, "destination_type": "queue"}, {"vhost": "workflow", "destination": "flow_add_tokens", "routing_key": "flow.place.add_tokens", "source": "workflow", "arguments": {}, "destination_type": "queue"}, {"vhost": "workflow", "destination": "dead_flow_add_tokens", "routing_key": "flow.place.add_tokens", "source": "dead", "arguments": {}, "destination_type": "queue"}, {"vhost": "workflow", "destination": "lsf_submit", "routing_key": "genome.execute.submit", "source": "workflow", "arguments": {}, "destination_type": "queue"}, {"vhost": "workflow", "destination": "dead_lsf_submit", "routing_key": "genome.execute.submit", "source": "dead", "arguments": {}, "destination_type": "queue"}, {"vhost": "workflow", "destination": "petri_notify_transition", "routing_key": "petri.transition.notify", "source": "workflow", "arguments": {}, "destination_type": "queue"}, {"vhost": "workflow", "destination": "dead_petri_notify_transition", "routing_key": "petri.transition.notify", "source": "dead", "arguments": {}, "destination_type": "queue"}, {"vhost": "workflow", "destination": "flow_execute_node", "routing_key": "flow.node.execute", "source": "workflow", "arguments": {}, "destination_type": "queue"}, {"vhost": "workflow", "destination": "dead_flow_execute_node", "routing_key": "flow.node.execute", "source": "dead", "arguments": {}, "destination_type": "queue"}, {"vhost": "workflow", "destination": "flow_fire_transition", "routing_key": "flow.transition.fire", "source": "workflow", "arguments": {}, "destination_type": "queue"}, {"vhost": "workflow", "destination": "dead_flow_fire_transition", "routing_key": "flow.transition.fire", "source": "dead", "arguments": {}, "destination_type": "queue"}], "rabbit_version": "3.0.1", "permissions": [{"write": ".*", "vhost": "/", "read": ".*", "user": "guest", "configure": ".*"}, {"write": ".*", "vhost": "workflow", "read": ".*", "user": "guest", "configure": ".*"}, {"write": ".*", "vhost": "testing", "read": ".*", "user": "guest", "configure": ".*"}]}"""



if __name__ == "__main__":
    unittest.main()
