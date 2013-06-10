import os

# Success and Failure are normal modes of operation
EXECUTE_SUCCESS = os.EX_OK
EXECUTE_FAILURE = 1

# Error means there is probably a bug and we should reject doing this work.
EXECUTE_ERROR = os.EX_SOFTWARE

# System failure means there is probably system instability and we
#   should retry.  Retrying usually means re-delivering a messages and/or
#   restarting a service.
EXECUTE_SYSTEM_FAILURE = os.EX_TEMPFAIL

EXECUTE_SERVICE_UNAVAILABLE = os.EX_UNAVAILABLE

UNKNOWN_ERROR = 1
