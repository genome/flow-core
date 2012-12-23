#!/usr/bin/env bash

PYTHONPATH=lib/:$PYTHONPATH

echo "Running unit tests..."
python -m discover -t . -s unit_tests
