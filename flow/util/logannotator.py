from twisted.internet import protocol
from twisted.internet import reactor
from time import strftime
import os
import sys

def write_output(fd, data, newline_pending):
    if fd is None or not data:
        return

    now = strftime("%Y/%m/%d %H:%M:%S")

    lines = data.splitlines(True)
    if newline_pending is True:
        fd.write(lines[0])
        lines = lines[1:]

    if lines:
        newline_pending = lines[-1][-1] != '\n'

        for line in lines:
            fd.write("[%s] %s" % (now, line))

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
            self.stderr_fd.close()
            self.stderr_fd = None

    def outReceived(self, data):
        try:
            self.stdout_newline_pending = write_output(self.stdout_fd, data,
                    self.stdout_newline_pending)
        except IOError as ex:
            self.stdout_fd.close()
            self.stdout_fd = None
            self.transport.loseConnection()

    def processExited(self, reason):
        self.exit_code = reason.value.exitCode

    def processEnded(self, reason):
        self.exit_code = reason.value.exitCode
        reactor.stop()

    def start(self):
        reactor.spawnProcess(self, self.cmdline[0], self.cmdline,
                env=os.environ, childFDs={0: 0, 1: "r", 2: "r"})
        reactor.run()
        return self.exit_code
