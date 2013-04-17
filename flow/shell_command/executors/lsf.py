from flow.configuration.settings.injector import setting
from flow.shell_command import util
from flow.shell_command.executors.base import ExecutorBase
from flow.shell_command.resource import Resource, ResourceException
from injector import inject
from pythonlsf import lsf
from twisted.python.procutils import which

import logging
import os


LOG = logging.getLogger(__name__)


_RESOURCE_MAP = {
        "min_proc": Resource(name="ncpus", type="int", units=None,
                operator=">=", reservable=False),

        "memory": Resource(name="mem", type="memory", units="MiB",
                operator=">=", reservable=True),

        "temp_space": Resource(name="gtmp", type="memory", units="GiB",
                operator=">=", reservable=True),
        }


def _select_item(name, value):
    resource = _RESOURCE_MAP[name]
    return "%s%s%s" % (resource.name, resource.operator, value)

def _rusage_item(name, value):
    resource = _RESOURCE_MAP[name]
    if not resource.reservable:
        raise ResourceException(
                "Attempted to reserve non-reservable resource %s" %
                resource.name)
    return "%s=%s" % (resource.name, value)

def _make_rusage_string(require, reserve):
    select = []
    for k, v in require.iteritems():
        select.append(_select_item(k, v))

    rusage = []
    for k, v in reserve.iteritems():
        if k not in require:
            select.append(_select_item(k, v))
        rusage.append(_rusage_item(k, v))

    rv = []
    if select:
        rv.append("select[%s]" % " && ".join(select))

    if rusage:
        rv.append("rusage[%s]" % ":".join(rusage))

    # we do not want this to be unicode (LSF will crash)
    return str(" ".join(rv))


@inject(post_exec=setting('shell_command.lsf.post_exec'),
        default_queue=setting('shell_command.lsf.default_queue'))
class LSFExecutor(ExecutorBase):
    def execute(self, command_line, **kwargs):
        request = self.construct_request(command_line, **kwargs)

        reply = _create_reply()

        try:
            submit_result = lsf.lsb_submit(request, reply)
        except:
            LOG.exception("lsb_submit failed for command string: '%s'",
                    command_line)
            raise

        if submit_result > 0:
            LOG.debug('successfully submitted lsf job: %s', submit_result)
            return True, submit_result
        else:
            lsf.lsb_perror("lsb_submit")
            LOG.error('failed to submit lsf job, return value = (%s), err = %s',
                    submit_result, lsf.lsb_sperror("lsb_submit"))
            return False, submit_result

    def construct_request(self, command_line,
            net_key=None, response_places=None, with_inputs=None,
            with_outputs=None, **lsf_kwargs):
        LOG.info("lsf kwargs: %r", lsf_kwargs)

        if self.post_exec is not None:
            executable_name = self.post_exec[0]
            args = " ".join(self.post_exec[1:])

            executable = _find_executable(executable_name)
            failure_place = response_places.pop('execute_failure')

            post_exec_cmd = create_post_exec_cmd(executable=executable,
                    args=args, net_key=net_key, failure_place=failure_place,
                    **lsf_kwargs)
            LOG.debug("lsf post_exec_cmd = '%s'", post_exec_cmd)
        else:
            post_exec_cmd = None

        request = create_request(post_exec_cmd=post_exec_cmd,
                default_queue=self.default_queue, **lsf_kwargs)

        full_command_line = self._make_command_line(command_line,
                net_key=net_key, response_places=response_places,
                with_inputs=with_inputs, with_outputs=with_outputs)
        command_string = ' '.join(map(str, full_command_line))
        LOG.debug("lsf command_string = '%s'", command_string)
        request.command = command_string

        return request


def create_request(post_exec_cmd, default_queue,
        name=None, queue=None, group=None, stdout=None, stderr=None,
        beginTime=0, mail_user=None, working_directory=None, project=None,
        resources={}, **other_lsf_kwargs):

    request = lsf.submit()
    request.options = 0
    request.options2 = 0
    request.options3 = 0

    if name:
        request.jobName = str(name)
        request.options |= lsf.SUB_JOB_NAME

    if mail_user:
        request.mailUser = str(mail_user)
        request.options |= lsf.SUB_MAIL_USER
        LOG.debug('setting mail_user = %s', mail_user)

    if queue:
        request.queue = str(queue)
    else:
        request.queue = default_queue
    request.options |= lsf.SUB_QUEUE
    LOG.debug("request.queue = %s", request.queue)

    #if group:
        #request.jobGroup = str(group)
        #request.options2 |= lsf.SUB_JOB_GROUP

    if project:
        request.projectName = str(project)
        request.options |= lsf.SUB_PROJECT_NAME

    if stdout:
        request.outFile = str(util.join_path_if_rel(
            working_directory, stdout))
        request.options |= lsf.SUB_OUT_FILE
        LOG.debug('setting job stdout = %s', stdout)
    if stderr:
        request.errFile = str(util.join_path_if_rel(
            working_directory, stderr))
        request.options |= lsf.SUB_ERR_FILE
        LOG.debug('setting job stderr = %s', stderr)

    if working_directory:
        request.cwd = str(working_directory)
        request.options3 |= lsf.SUB3_CWD
        LOG.debug('setting cwd = %s', working_directory)

    reserve = resources.get("reserve", {})
    require = resources.get("require", {})
    limit = resources.get("limit", {})

    if post_exec_cmd:
        request.postExecCmd = str(post_exec_cmd)
        request.options3 |= lsf.SUB3_POST_EXEC

    numProcessors = require.get("min_proc", 1)
    maxNumProcessors = require.get("max_proc", numProcessors)

    termTime = limit.get("cpu_time", 0)

    request.numProcessors = int(numProcessors)
    request.maxNumProcessors = int(maxNumProcessors)

    request.beginTime = int(beginTime)
    request.termTime = int(termTime)

    if require or reserve:
        rusage = _make_rusage_string(require=require, reserve=reserve)
        LOG.debug('setting resource request string = %r', rusage)
        request.options |= lsf.SUB_RES_REQ
        request.resReq = rusage

    rlimits = resources.get("limit", {})
    LOG.info("Setting rlimits: %r", rlimits)
    request.rLimits = get_rlimits(**rlimits)

    return request


def get_rlimits(max_resident_memory=None, max_virtual_memory=None,
        max_processes=None, max_threads=None, max_open_files=None,
        max_stack_size=None):
    # Initialize unused limits
    limits = [lsf.DEFAULT_RLIMIT] * lsf.LSF_RLIM_NLIMITS

    if max_resident_memory:
        max_resident_memory = int(max_resident_memory)
        limits[lsf.LSF_RLIMIT_RSS] = max_resident_memory
        LOG.debug('setting rLimit for max_resident_memory to %r',
                max_resident_memory)

    if max_virtual_memory:
        limits[lsf.LSF_RLIMIT_VMEM] = int(max_virtual_memory)
        LOG.debug('setting rLimit for max_virtual_memory to %r',
                max_virtual_memory)

    if max_processes:
        limits[lsf.LSF_RLIMIT_PROCESS] = int(max_processes)
        LOG.debug('setting rLimit for max_processes to %r', max_processes)

    if max_threads:
        limits[lsf.LSF_RLIMIT_THREAD] = int(max_threads)
        LOG.debug('setting rLimit for max_threads to %r', max_threads)

    if max_open_files:
        limits[lsf.LSF_RLIMIT_NOFILE] = int(max_open_files)
        LOG.debug('setting rLimit for max_open_files to %r', max_open_files)

    if max_stack_size:
        limits[lsf.LSF_RLIMIT_STACK] = int(max_stack_size)
        LOG.debug('setting rLimit for max_stack_size to %r', max_stack_size)

    return limits


def _create_reply():
    reply = lsf.submitReply()

    init_code = lsf.lsb_init('')
    if init_code > 0:
        raise RuntimeError("Failed lsb_init, errno = %d" % lsf.lsb_errno())

    return reply

def create_post_exec_cmd(executable, args, net_key, failure_place,
        stderr=None, stdout=None, **other_lsf_kwargs):

    cmd_string = "'%s' %s -n '%s' -f %s" % (
            executable, args, net_key, failure_place)
    result = "bash -c \"%s\" 1>> '%s' 2>> '%s'" % (cmd_string, stdout, stderr)
    return result

def _find_executable(name):
    executables = which(name)
    if executables:
        return executables[0]
    else:
        msg = "Couldn't find the executable by name when" +\
                " looking in PATH=%s for %s" % (os.environ.get('PATH', None),
                name)
        LOG.warning(msg)
        raise RuntimeError(msg)
