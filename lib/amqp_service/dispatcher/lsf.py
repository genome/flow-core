import logging
from pythonlsf import lsf

from amqp_service.dispatcher import util

LOG = logging.getLogger(__name__)

class LSFDispatcher(object):
    def __init__(self, default_queue='gms'):
        self.default_queue = default_queue

    def launch_job(self, command, env={}, **kwargs):
        command_string = set_command_string(request, command, **kwargs)
        request = self.create_request(command_string, **kwargs)
        reply = _create_reply()

        with util.environment(env):
            submit_result = lsf.lsb_submit(request, reply)

        if submit_result > 0:
            return True, submit_result
        else:
            return False, None


    def create_request(self, command_string, queue=None,
            stdout=None, stderr=None, beginTime=0, termTime=0,
            numProcessors=1, maxNumProcessors=1, **kwargs):
        request = lsf.submit()
        request.options = 0
        request.options2 = 0

        request.command = command_string

        if queue:
            request.queue = queue
        else:
            request.queue = self.default_queue
        request.options += lsf.SUB_QUEUE
        LOG.debug("request.queue = %s", request.queue)

        if stdout:
            request.outFile = stdout
            request.options += lsf.SUB_OUT_FILE
            LOG.debug('setting job stdout = %s', stdout)
        if stderr:
            request.errFile = stderr
            request.options += lsf.SUB_ERR_FILE
            LOG.debug('setting job stderr = %s', stderr)

        request.beginTime = int(beginTime)
        request.termTime = int(termTime)

        request.numProcessors = numProcessors
        request.maxNumProcessors = maxNumProcessors

        request.rLimits = get_rlimits(**kwargs)

        return request


def set_command_string(request, command, arguments=[],
        wrapper=None, wrapper_arguments=[], **kwargs):
    command_list = []
    if wrapper:
        request.jobName = command
        request.options += lsf.SUB_JOB_NAME
        command_list.append(wrapper)
        command_list.extend(wrapper_arguments)

    command_list.append(command)
    command_list.extend(arguments)
    command_string = ' '.join(map(str, command_list))
    LOG.debug("command_string = '%s'", command_string)

    return command_string


def get_rlimits(max_resident_memory=None, max_virtual_memory=None,
        max_processes=None, max_threads=None, max_open_files=None,
        max_stack_size=None, **kwargs):
    # Initialize unused limits
    limits = [lsf.DEFAULT_RLIMIT
            for i in xrange(lsf.LSF_RLIM_NLIMITS)]

    if max_resident_memory:
        limits[lsf.LSF_RLIMIT_RSS] = int(max_resident_memory)
        LOG.debug('setting rLimit for max_resident_memory to %d',
                max_resident_memory)

    if max_virtual_memory:
        limits[lsf.LSF_RLIMIT_VMEM] = int(max_virtual_memory)
        LOG.debug('setting rLimit for max_virtual_memory to %d',
                max_virtual_memory)

    if max_processes:
        limits[lsf.LSF_RLIMIT_PROCESS] = int(max_processes)
        LOG.debug('setting rLimit for max_processes to %d', max_processes)

    if max_threads:
        limits[lsf.LSF_RLIMIT_THREAD] = int(max_threads)
        LOG.debug('setting rLimit for max_threads to %d', max_threads)

    if max_open_files:
        limits[lsf.LSF_RLIMIT_NOFILE] = int(max_open_files)
        LOG.debug('setting rLimit for max_open_files to %d', max_open_files)

    if max_stack_size:
        limits[lsf.LSF_RLIMIT_STACK] = int(max_stack_size)
        LOG.debug('setting rLimit for max_stack_size to %d', max_stack_size)

    return limits


def _create_reply():
    reply = lsf.submitReply()

    init_code = lsf.lsb_init('')
    if init_code > 0:
        raise RuntimeError("Failed lsb_init, errno = %d" % lsf.lsb_errno())

    return reply
