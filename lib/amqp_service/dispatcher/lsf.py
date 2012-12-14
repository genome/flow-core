import logging
from pythonlsf import lsf

from amqp_service.dispatcher import util

LOG = logging.getLogger(__name__)

class LSFDispatcher(object):
    def __init__(self, default_queue='gms'):
        self.default_queue = default_queue

    def get_job_status(self, job_id):
        pass

    def launch_job(self, command, arguments=[],
            wrapper=None, wrapper_args=[], env={},
            queue=None, stdout=None, stderr=None,
            beginTime=0, termTime=0,
            numProcessors=1, maxNumProcessors=1,
            max_resident_memory=None, max_virtual_memory=None,
            max_processes=None, max_threads=None,
            max_open_files=None, max_stack_size=None):
        request = lsf.submit()
        request.options = 0
        request.options2 = 0

        command_list = []
        if wrapper:
            request.jobName = command
            request.options += lsf.JOB_NAME
            command_list.append(wrapper)
            command_list.extend(wrapper_args)

        command_list.append(command)
        command_list.extend(arg)
        request.command = ' '.join(map(str, command_list))
        LOG.debug("request.command = '%s'", request.command)

        if queue:
            request.queue = str(queue)
        else:
            request.queue = self.default_queue
        request.options += lsf.SUB_QUEUE
        LOG.debug("request.queue = %s", request.queue)

        if stdout:
            request.outFile = str(stdout)
            request.options += lsf.SUB_OUT_FILE
            LOG.debug('setting job stdout = %s', stdout)
        if stderr:
            request.errFile = str(stderr)
            request.options += lsf.SUB_ERR_FILE
            LOG.debug('setting job stderr = %s', stderr)

        request.beginTime = int(beginTime)
        request.termTime = int(termTime)

        request.numProcessors = numProcessors
        request.maxNumProcessors = maxNumProcessors

        # Initialize unused limits
        limits = []
        for i in range(lsf.LSF_RLIM_NLIMITS):
            limits.append(lsf.DEFAULT_RLIMIT)
        request.rLimits = limits

        if max_resident_memory:
            request.rLimits[lsf.LSF_RLIMIT_RSS] = int(max_resident_memory)
            LOG.debug('setting rLimit for max_resident_memory to %d',
                    max_resident_memory)

        if max_virtual_memory:
            request.rLimits[lsf.LSF_RLIMIT_VMEM] = int(max_virtual_memory)
            LOG.debug('setting rLimit for max_virtual_memory to %d',
                    max_virtual_memory)

        if max_processes:
            request.rLimits[lsf.LSF_RLIMIT_PROCESS] = int(max_processes)
            LOG.debug('setting rLimit for max_processes to %d', max_processes)

        if max_threads:
            request.rLimits[lsf.LSF_RLIMIT_THREAD] = int(max_threads)
            LOG.debug('setting rLimit for max_threads to %d', max_threads)

        if max_open_files:
            request.rLimits[lsf.LSF_RLIMIT_NOFILE] = int(max_open_files)
            LOG.debug('setting rLimit for max_open_files to %d', max_open_files)

        if max_stack_size:
            request.rLimits[lsf.LSF_RLIMIT_STACK] = int(max_stack_size)
            LOG.debug('setting rLimit for max_stack_size to %d', max_stack_size)


        reply = lsf.submitReply()

        init_code = lsf.lsb_init('')
        if init_code > 0:
            raise RuntimeError("Failed lsb_init, errno = %d" % lsf.lsb_errno())

        with util.environment(env):
            submit_result = lsf.lsb_submit(request, reply)

        if submit_result > 0:
            return True, submit_result
        else:
            return False, None
