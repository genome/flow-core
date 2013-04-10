from twisted.internet import protocol
from twisted.internet import reactor
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
        newline_pending = lines[-1][-1] != '\n'

        for line in lines:
            fd.write("%s[%s] %s" % (prefix, now, line))

    return newline_pending


class LogAnnotator(protocol.ProcessProtocol):
    def __init__(self, cmdline, stdout_fd=sys.stdout, stderr_fd=sys.stderr):
        self.cmdline = cmdline
        self.stdout_fd = stdout_fd
        self.stderr_fd = stderr_fd

        self.stdout_newline_pending = False
        self.stderr_newline_pending = False

        self.exit_code = 1

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
        self.exit_code = reason.value.exitCode
        reactor.stop()

    def start(self):
        hostname = socket.gethostname()
        self.announce_host()
        reactor.spawnProcess(self, self.cmdline[0], self.cmdline,
                env=os.environ, childFDs={0:0, 1:'r', 2:'r'})
        reactor.run()
        return self.exit_code

    def announce_host(self):
        msg = "Starting log annotation on host: %s\n" % hostname
        write_output(self.stdout_fd, msg, self.stdout_newline_pending)
        if self.stdout_fd != self.stderr_fd:
            write_output(self.stderr_fd, msg, self.stderr_newline_pending)

if __name__ == '__main__':
    log_annotator = LogAnnotator(sys.argv[1:])
    log_annotator.start()
