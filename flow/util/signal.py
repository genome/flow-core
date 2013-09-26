from flow.util.exit import exit_process

import logging
import signal


LOG = logging.getLogger(__name__)


def setup_standard_signal_handlers():
    setup_exit_handler(signal.SIGINT, [signal.SIGINT, signal.SIGTERM])
    setup_exit_handler(signal.SIGTERM, [signal.SIGTERM])


def setup_exit_handler(signum, child_signals):
    def _handler(signum, frame):
        LOG.critical('Received signal %d: %s', signum, frame)
        exit_process(exit_codes.UNKNOWN_ERROR, child_signals=child_signals)
    signal.signal(signum, _handler)
