#!/usr/bin/env bash

PYTHONPATH=lib/:$PYTHONPATH
TESTING_AMQP_URL='ampq://guest:guest@vmpool82:5672/testing'

export TESTING_AMQP_URL

echo "Deleting stale *.pyc files..."
find . -name "*.pyc" -delete

echo "Running unit tests..."
python -m discover -t . -s unit_tests || exit 0

echo
echo "Running integration tests..."
python -m discover -t . -s integration_tests || exit 0

echo
echo "Running system tests..."
python -m discover -t . -s system_tests || exit 0
