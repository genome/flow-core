#!/usr/bin/env bash

PYTHONPATH=lib/:$PYTHONPATH

echo "Running unit tests..."
python -m discover -t . -s unit_tests

echo
echo "Running integration tests..."
python -m discover -t . -s integration_tests
