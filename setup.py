from setuptools import setup, find_packages

import glob
import os.path

entry_points = '''
[console_scripts]
flow = flow.main:main
flow-fork-shell-command-executor = flow.shell_command.fork.executor:main
flow-lsf-shell-command-executor = flow.shell_command.lsf.executor:main
flow-redis-server = flow.local_redis:main


[flow.commands]
orchestrator = flow.orchestrator.command:OrchestratorCommand

fork-shell-command-service = flow.shell_command.fork.commands.service:ForkShellCommand
lsf-shell-command-service = flow.shell_command.lsf.commands.service:LSFShellCommand
lsf-post-exec = flow.shell_command.lsf.commands.post_exec:LsfPostExecCommand
lsf-pre-exec = flow.shell_command.lsf.commands.pre_exec:LsfPreExecCommand
shell-command-wrapper = flow.shell_command.commands.wrapper:WrapperCommand

configure-rabbitmq = flow.commands.configurerabbitmq:ConfigureRabbitMQCommand
console = flow.commands.console:ConsoleCommand

rabbit = flow.commands.rabbit:RabbitCommand

monitoring-command = flow.pmon.test_commands.monitoring_command:MonitoringCommand
reading-command = flow.pmon.test_commands.reading_command:ReadingCommand
random-reading-command = flow.pmon.test_commands.reading_command:RandomReadingCommand

benchmark = flow.commands.benchmark:BenchmarkCommand

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
            'flow': ['flow/local-redis.conf'],
        },
        entry_points = entry_points,
        install_requires = [
            'blist',
            'hiredis',
            'injector',
            'ipython',
            'pika',
            'platform-python-lsf-api',
            'psutil',
            'python-magic',
            'pyyaml',
            'redis',
            'requests',  # for flow rabbit
            'statsd-client',
            'twisted==12.3.99',
        ],
        dependency_links=[
            'https://github.com/genome-vendor/twisted/tarball/twisted-12.3.99#egg=Twisted-12.3.99',
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
