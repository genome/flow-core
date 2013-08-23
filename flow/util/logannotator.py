from twisted.internet import protocol, reactor, defer
from datetime import datetime
import socket
import os
import sys

def write_output(fd, data, newline_pending, prefix=''):
    if fd is None or not data:
        return

    now = datetime.now().strftime("%Y/%m/%d %H:%M:%S.%f")

    lines = data.splitlines(True)
    if newline_pending is True:
        fd.write(lines[0])
        lines = lines[1:]

    if lines:
        newline_pending = not lines[-1].endswith('\n')

        for line in lines:
            fd.write("%s[%s] %s" % (prefix, now, line))

    else:
        newline_pending = False

    fd.flush()

    return newline_pending


class LogAnnotator(protocol.ProcessProtocol):
    def __init__(self, cmdline, stdout_fd=sys.stdout, stderr_fd=sys.stderr,
            log_hostname=True):

        self.deferred = defer.Deferred()
        self.cmdline = cmdline
        self.stdout_fd = stdout_fd
        self.stderr_fd = stderr_fd
        self.log_hostname = log_hostname

        self.stdout_newline_pending = False
        self.stderr_newline_pending = False

    def connectionMade(self):
        pass

    def errReceived(self, data):
        try:
            self.stderr_newline_pending = write_output(self.stderr_fd, data,
                    self.stderr_newline_pending)
        except IOError as ex:
            write_output(self.stderr_fd, ex, self.stderr_newline_pending)
            self.stderr_fd.close()
            self.stderr_fd = None
            self.transport.loseConnection()

    def outReceived(self, data):
        try:
            self.stdout_newline_pending = write_output(self.stdout_fd, data,
                    self.stdout_newline_pending)
        except IOError as ex:
            write_output(self.stderr_fd, ex, self.stderr_newline_pending)
            self.stdout_fd.close()
            self.stdout_fd = None
            self.transport.loseConnection()

    def processEnded(self, reason):
        exit_code = reason.value.exitCode
        self.deferred.callback(exit_code)

    def start(self):
        """
        Returns a deferred that will callback (with the exit_code) when the
        process exits.
        """
        if self.log_hostname:
            self.announce_hostname()
        reactor.spawnProcess(self, self.cmdline[0], self.cmdline,
                env=os.environ, childFDs={0:0, 1:'r', 2:'r'})
        return self.deferred

    def announce_hostname(self):
        hostname = socket.gethostname()
        msg = "Starting log annotation on host: %s\n" % hostname
        write_output(self.stdout_fd, msg, self.stdout_newline_pending)
        if self.stdout_fd != self.stderr_fd:
            write_output(self.stderr_fd, msg, self.stderr_newline_pending)


if __name__ == '__main__':
    log_annotator = LogAnnotator(sys.argv[1:])
    d = log_annotator.start()
    d.addCallback(lambda x: reactor.stop())
    d.addErrback(lambda x: reactor.stop())
    reactor.run()
