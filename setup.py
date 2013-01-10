import distribute_setup
distribute_setup.use_setuptools()

from setuptools import setup, find_packages

entry_points = '''
[flow.protocol.message_classes]
submit_command = flow_command_runner.messages:CommandLineSubmitMessage
command_result = flow_command_runner.messages:CommandLineResponseMessage
execute_node = flow.orchestrator.messages:ExecuteNodeMessage
'''

setup(
        name = 'flow',
        version = '0.1',
        packages = find_packages(exclude=['unit_tests']),
        entry_points = entry_points,
        install_requires = [
            'amqp_manager',
            'argparse',
            'platform-python-lsf-api',
            'pygraphviz',
            'pyyaml',
            'redis',
        ],
        tests_require = [
            'mock',
        ],
        test_suite = 'unit_tests',
)
