import distribute_setup
distribute_setup.use_setuptools()

from setuptools import setup, find_packages

setup(
        name = 'amqp_manager',
        version = '0.1',
        packages = find_packages(exclude=[
            'unit_tests',
            'integration_tests',
            'system_tests'
        ]),
        install_requires = [
            'pika',
        ],
        tests_require = [
            'mock',
        ],
        test_suite = 'unit_tests',
)
