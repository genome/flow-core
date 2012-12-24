#!/usr/bin/env bash

PYTHONPATH=lib/:$PYTHONPATH
TESTING_AMQP_URL='ampq://guest:guest@vmpool82:5672/testing'

export TESTING_AMQP_URL

echo "Running unit tests..."
python -m discover -t . -s unit_tests

echo
echo "Running integration tests..."
python -m discover -t . -s integration_tests

echo
echo "Running system tests..."
python -m discover -t . -s system_tests
