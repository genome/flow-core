import logging
import syslog

_SYSLOG_PRIORITIES = {
        'DEBUG':    syslog.LOG_DEBUG,
        'INFO':     syslog.LOG_NOTICE,
        'WARNING':  syslog.LOG_WARNING,
        'ERROR':    syslog.LOG_ERR,
        'CRITICAL': syslog.LOG_CRIT,
}

class CompliantSyslogHandler(logging.Handler):
    def __init__(self, ident, syslog_options=syslog.LOG_CONS,
            facility=syslog.LOG_USER):
        '''
        This logging handler provides fewer options than the default Python
        SysLogHandler, but actually conforms to the syslog protocol (instead of
        just spamming text to the syslog port).

        This makes it easy to get JSON messages pushed to our logging
        aggregator.
        '''
        logging.Handler.__init__(self)
        syslog.openlog(ident, syslog_options, facility)

    def emit(self, record):
        msg = self.format(record)
        syslog.syslog(_SYSLOG_PRIORITIES[record.levelname], msg)
