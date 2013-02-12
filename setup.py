from setuptools import setup, find_packages

entry_points = '''
[flow.protocol.message_classes]
execute_node = flow.orchestrator.messages:ExecuteNodeMessage
add_tokens = flow.petri.safenet:SetTokenMessage
notify_transition = flow.petri.safenet:NotifyTransitionMessage
status_request = flow.orchestrator.messages:NodeStatusRequestMessage
status_response = flow.orchestrator.messages:NodeStatusResponseMessage
submit_command = flow.command_runner.messages:CommandLineSubmitMessage
command_result = flow.command_runner.messages:CommandLineResponseMessage

[flow.commands]
status = flow.commands.status:StatusCommand
set-token = flow.commands.set_token:SetTokenCommand
orchestrator = flow.commands.service:ServiceCommand
local_command_line_service = flow.commands.service:ServiceCommand
lsf_command_line_service = flow.commands.service:ServiceCommand
command_line_wrapper = flow.commands.wrapper:WrapperCommand
configure_rabbitmq = flow.commands.configurerabbitmq:ConfigureRabbitMQCommand

[flow.factories]
status_command = flow.commands.status:StatusCommand


dictionary_factory = flow.factories:dictionary_factory

redis_storage_singleton = flow.storage:redis_storage_singleton

asynchronous_amqp_broker = flow.brokers.strategic_broker:StrategicAmqpBroker
blocking_broker = flow.brokers.blocking:BlockingAmqpBroker
publisher_confirm_acking = flow.brokers.acking_strategies:PublisherConfirmation
immediate_acking = flow.broker.acking_strategies:Immediate

orchestrator_service_interface = flow.orchestrator.client:OrchestratorClient
shell_command_service_interface = flow.command_runner.client:CommandLineClient

execute_node_handler = flow.orchestrator.handlers:ExecuteNodeHandler
node_status_request_handler = flow.orchestrator.handlers:NodeStatusRequestHandler
method_descriptor_handler = flow.orchestrator.handlers:MethodDescriptorHandler
petri_set_token_handler = flow.orchestrator.handlers:PetriSetTokenHandler
petri_notify_transition_handler = flow.orchestrator.handlers:PetriNotifyTransitionHandler

command_line_submit_handler = flow.command_runner.handler:CommandLineSubmitMessageHandler
command_line_local_executor = flow.command_runner.executors.local:SubprocessExecutor
command_line_lsf_executor = flow.command_runner.executors.lsf:LSFExecutor
'''

setup(
        name = 'flow',
        version = '0.1',
        packages = find_packages(exclude=[
            'unit_tests',
            'integration_tests',
            'system_tests',
            'test_helpers'
        ]),
        entry_points = entry_points,
        install_requires = [
            'blist',
            'pika',
            'platform-python-lsf-api',
            'pygraphviz',
            'pyyaml',
            'redis',
        ],
        setup_requires = [
            'nose',
        ],
        tests_require = [
            'blist',
            'mock',
            'nose',
            'coverage',
            'fakeredis',
        ],
        test_suite = 'unit_tests',
)
