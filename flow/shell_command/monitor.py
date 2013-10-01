from twisted.internet import defer, error, protocol

import json
import logging
import sys


LOG = logging.getLogger(__name__)


class ExecutorMonitor(protocol.ProcessProtocol):
    def __init__(self, data_to_send, log_file=None, uid=None, gid=None):
        self.data_to_send = data_to_send
        self.log_file = log_file

        if uid is None:
            self.uid = -1
        else:
            self.uid = uid

        if gid is None:
            self.gid = -1
        else:
            self.gid = gid

        self.job_id_deferred = defer.Deferred()
        self.job_ended_deferred = defer.Deferred()

        self._job_id_data = []
        self._log_file_handle = None

    def connectionMade(self):
        self.transport.write(json.dumps(self.data_to_send))
        self.transport.closeStdin()

    @property
    def log_file_handle(self):
        if not self._log_file_handle:
            try:
                LOG.debug('Trying to open log file in %s: %s',
                        self.__class__.__name__, self.log_file)
                if self.log_file:
                    try:
                        self._log_file_handle = open(self.log_file, 'a')
                        self._set_log_file_ownership()
                    except OSError:
                        # try to write to the actual log file in the future
                        return sys.stderr
                else:
                    self._log_file_handle = sys.stderr
            except:
                LOG.info('Could not open log file %r', self.log_file)
                self._log_file_handle = sys.stderr

        return self._log_file_handle

    def _set_log_file_ownership(self):
        os.chown(self.log_file, self.uid, self.gid)
        os.chmod(self.log_file, 0664)

    def close_log_file(self):
        if self._log_file_handle:
            self._log_file_handle.close()
            self._log_file_handle = None


    def childDataReceived(self, childFD, data):
        if childFD == 3:
            self._job_id_data.append(data)
        else:
            self.log_file_handle.write(data)
            self.log_file_handle.flush()

    def childConnectionLost(self, childFD):
        if childFD == 3:
            self._job_id_connection_lost()
        else:
            LOG.warning('Lost connection with child FD %d', childFD)

    def _job_id_connection_lost(self):
        job_id = ''.join(self._job_id_data)
        if job_id:
            self.job_id_deferred.callback(job_id)
        else:
            self.job_id_deferred.errback(RuntimeError('No job id received'))

    def processEnded(self, exit_status):
        try:
            if isinstance(exit_status.value, error.ProcessDone):
                self.job_ended_deferred.callback(None)
            else:
                self.log_file_handle.write('Fork service execution failed: %s\n'
                        % exit_status.value)
                self.log_file_handle.flush()
                self.job_ended_deferred.errback(exit_status)
        finally:
            self.close_log_file()
