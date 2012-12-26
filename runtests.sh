#!/usr/bin/env bash

# Configurable options
export TESTING_AMQP_URL='ampq://guest:guest@vmpool82:5672/testing'

DEFAULT_DELETE_STALE_PYC=true
DEFAULT_RUN_UNIT_TESTS=true
DEFAULT_RUN_INTEGRATION_TESTS=true
DEFAULT_RUN_SYSTEM_TESTS=true

DELETE_STALE_PYC=false
RUN_UNIT_TESTS=false
RUN_INTEGRATION_TESTS=false
RUN_SYSTEM_TESTS=false


# Non-configurable variables
ARGUMENTS_RECEIVED=false

export PYTHONPATH=lib/:$PYTHONPATH


display_args() {
    echo "Options:" >&2
    echo >&2
    echo "    -a    Run all tests." >&2
    echo >&2
    echo "    -d    Delete stale *.pyc files." >&2
    echo "    -u    Run unit tests." >&2
    echo "    -i    Run integration tests." >&2
    echo "    -s    Run system tests." >&2
    echo >&2
    echo "    -h    Print this message." >&2
    echo >&2
}

delete_stale_pyc() {
    echo "Deleting stale *.pyc files..."
    find . -name "*.pyc" -delete
}

run_unit_tests() {
    echo "Running unit tests..."
    python -m discover -t . -s unit_tests
    echo
}

run_integration_tests() {
    echo "Running integration tests..."
    python -m discover -t . -s integration_tests
    echo
}

run_system_tests() {
    echo "Running system tests..."
    python -m discover -t . -s system_tests
    echo
}

while getopts "aduish" FLAG; do
    case $FLAG in
        "a")
            ARGUMENTS_RECEIVED=true
            DELETE_STALE_PYC=true
            RUN_UNIT_TESTS=true
            RUN_INTEGRATION_TESTS=true
            RUN_SYSTEM_TESTS=true;;
        "d")
            ARGUMENTS_RECEIVED=true
            DELETE_STALE_PYC=true;;
        "u")
            ARGUMENTS_RECEIVED=true
            RUN_UNIT_TESTS=true;;
        "i")
            ARGUMENTS_RECEIVED=true
            RUN_INTEGRATION_TESTS=true;;
        "s")
            ARGUMENTS_RECEIVED=true
            RUN_SYSTEM_TESTS=true;;
        "h")
            display_args
            exit 0;;
        \?)
            display_args
            exit 0
    esac
done

echo "Running automated tests..."
echo

if $ARGUMENTS_RECEIVED; then
    if $DELETE_STALE_PYC; then
        delete_stale_pyc
    fi

    if $RUN_UNIT_TESTS; then
        run_unit_tests
    fi

    if $RUN_INTEGRATION_TESTS; then
        run_integration_tests
    fi

    if $RUN_SYSTEM_TESTS; then
        run_system_tests
    fi

else
    if $DEFAULT_DELETE_STALE_PYC; then
        delete_stale_pyc
    fi

    if $DEFAULT_RUN_UNIT_TESTS; then
        run_unit_tests
    fi

    if $DEFAULT_RUN_INTEGRATION_TESTS; then
        run_integration_tests
    fi

    if $DEFAULT_RUN_SYSTEM_TESTS; then
        run_system_tests
    fi

fi

echo "Tests complete."
