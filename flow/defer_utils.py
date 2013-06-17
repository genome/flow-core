import os
from flow.exit_codes import EXECUTE_ERROR
import logging

LOG = logging.getLogger(__name__)

def catch_errors_and_crash(error):
    LOG.exception("Unexpected error in amqp_broker's channel facade.")
    os._exit(EXECUTE_ERROR)

def add_callback_and_default_errback(_deferred, _callback_fn, *args, **kwargs):
    _deferred.addCallback(_callback_fn, *args, **kwargs)
    _deferred.addErrback(catch_errors_and_crash)
    return _deferred
