from setuptools import setup, find_packages

import glob
import os.path

entry_points = '''
[console_scripts]
flow = flow.main:main

[flow.commands]
set-token = flow.commands.set_token:SetTokenCommand
orchestrator = flow.orchestrator.command:OrchestratorCommand

fork-shell-command-service = flow.shell_command.commands.fork_service:ForkShellCommand
lsf-shell-command-service = flow.shell_command.lsf.commands.service:LSFShellCommand
lsf-post-exec = flow.shell_command.lsf.commands.post_exec:LsfPostExecCommand
lsf-pre-exec = flow.shell_command.lsf.commands.pre_exec:LsfPostExecCommand
shell-command-wrapper = flow.shell_command.commands.wrapper:WrapperCommand

configure-rabbitmq = flow.commands.configurerabbitmq:ConfigureRabbitMQCommand
console = flow.commands.console:ConsoleCommand
graph = flow.commands.graph:GraphCommand

monitoring-command = flow.util.monitoring_command:MonitoringCommand
reading-command = flow.util.reading_command:ReadingCommand
random-reading-command = flow.util.reading_command:RandomReadingCommand

[flow.services]
orchestrator = flow.orchestrator.service_interface:OrchestratorServiceInterface
fork = flow.shell_command.service_interface:ForkShellCommandServiceInterface
lsf = flow.shell_command.service_interface:LSFShellCommandServiceInterface
'''

LUA_FILES = glob.glob(os.path.join(
    os.path.dirname(__file__), 'flow', 'petri_net', 'lua', '*'))

setup(
        name = 'flow',
        version = '0.1',
        packages = find_packages(exclude=[
            'unit_tests',
            'integration_tests',
            'system_tests',
            'test_helpers'
        ]),
        include_package_data=True,
        package_data={
            'flow/petri_net/lua': LUA_FILES,
        },
        entry_points = entry_points,
        install_requires = [
            'blist',
            'hiredis',
            'injector',
            'python-magic==0.4.3',
            'psutil==0.7.1',
            'ipython',
            'pika',
            'platform-python-lsf-api',
            'pygraphviz',
            'pyyaml',
            'redis',
            'statsd-client',
            'twisted',
        ],
        setup_requires = [
            'nose',
        ],
        tests_require = [
            'mock',
            'nose',
            'coverage',
            'fakeredis',
        ],
        test_suite = 'unit_tests',
)
