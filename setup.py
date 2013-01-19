from setuptools import setup, find_packages

entry_points = '''
[flow.protocol.message_classes]
submit_command = flow.command_runner.messages:CommandLineSubmitMessage
command_result = flow.command_runner.messages:CommandLineResponseMessage
execute_node = flow.orchestrator.messages:ExecuteNodeMessage
status_request = flow.orchestrator.messages:NodeStatusRequestMessage
status_response = flow.orchestrator.messages:NodeStatusResponseMessage
'''

setup(
        name = 'flow',
        version = '0.1',
        packages = find_packages(exclude=[
            'unit_tests',
            'integration_tests',
            'system_tests'
        ]),
        entry_points = entry_points,
        install_requires = [
            'argparse',
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
            'mock',
            'nose',
            'fakeredis',
        ],
        test_suite = 'unit_tests',
)
