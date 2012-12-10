import logging
from pythonlsf import lsf

from pprint import pprint

LOG = logging.getLogger(__name__)

class LSFDispatcher(object):
    def __init__(self, default_queue='gms'):
        self.default_queue = default_queue

    def get_job_status(self, job_id):
        pass

    def launch_job(self, command, arg=[], **kwargs):
        request = lsf.submit()

        command_list = [command]
        command_list.extend(arg)
        request.command = str(' '.join(command_list))

        request.options = 0
        request.options2 = 0
        request.queue = self.default_queue
        pprint(request.queue)

        request.beginTime = 0
        request.termTime = 0
        request.numProcessors = 1
        request.maxNumProcessors = 1

        limits = []
        for i in range(0, lsf.LSF_RLIM_NLIMITS):
            limits.append(lsf.DEFAULT_RLIMIT)

        request.rLimits = limits

        reply = lsf.submitReply()

        init_code = lsf.lsb_init("test")

        if init_code > 0:
            pprint(reply)
            raise RuntimeError("Something bad happened: %s" %init_code)
        else:
            return lsf.lsb_submit(request, reply)



