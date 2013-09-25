from flow.configuration.settings.injector import setting
from flow.interfaces import IServiceLocator
from flow.shell_command.handler_base import ShellCommandSubmitMessageHandler
from injector import inject


@inject(queue_name=setting('shell_command.lsf.queue'),
        exchange=setting('shell_command.lsf.exchange'),
        response_routing_key=setting('shell_command.lsf.response_routing_key'),
        service_interfaces=IServiceLocator)
class LSFShellCommandMessageHandler(ShellCommandSubmitMessageHandler):
    executable_name = 'flow-lsf-shell-command-executor'

    def on_job_id_success(self, job_id, callback_data=None,
            job_id_handled=None):
        dispatch_data = {'job_id': job_id}
        d = self.send_message('msg: dispatch_success',
            callback_data, token_data=dispatch_data)

        d.addCallbacks(job_id_handled.callback, job_id_handled.errback)

        return d

    def on_job_id_failure(self, error, callback_data=None,
            job_id_handled=None):
        d = self.send_message('msg: dispatch_failure', callback_data)
        d.addCallbacks(job_id_handled.callback, job_id_handled.errback)
        return d

    def on_job_ended_success(self, result, callback_data=None,
            job_ended_handled=None):
        job_ended_handled.callback(result)
        return job_ended_handled

    def on_job_ended_failure(self, error, callback_data=None,
            job_ended_handled=None):
        job_ended_handled.errback(error)
        return job_ended_handled
