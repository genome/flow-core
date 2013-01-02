import distribute_setup
distribute_setup.use_setuptools()

from setuptools import setup, find_packages

setup(
        name = 'amqp_manager',
        version = '0.1',
        packages = find_packages(),
        install_requires = [
            'pika',
        ],
        test_suite = 'unit_tests',
)
