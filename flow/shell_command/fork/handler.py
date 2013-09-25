from flow.configuration.settings.injector import setting
from flow.interfaces import IServiceLocator
from flow.shell_command.handler_base import ShellCommandSubmitMessageHandler
from injector import inject
from twisted.internet import defer

import socket


@inject(queue_name=setting('shell_command.fork.queue'),
        exchange=setting('shell_command.fork.exchange'),
        response_routing_key=setting('shell_command.fork.response_routing_key'),
        service_interfaces=IServiceLocator)
class ForkShellCommandMessageHandler(ShellCommandSubmitMessageHandler):
    executable_name = 'flow-fork-shell-command-executor'

    def on_job_id_success(self, job_id, callback_data=None,
            job_id_handled=None):
        deferreds = []
        dispatch_data = {'job_id': job_id}
        deferreds.append(self.send_message('msg: dispatch_success',
            callback_data, token_data=dispatch_data))

        execute_data = {'hostname': socket.gethostname()}
        deferreds.append(self.send_message('msg: execute_begin',
            callback_data, token_data=execute_data))

        dlist = defer.gatherResults(deferreds, consumeErrors=True)

        dlist.addCallbacks(job_id_handled.callback, job_id_handled.errback)

        return dlist

    def on_job_id_failure(self, error, callback_data=None,
            job_id_handled=None):
        d = self.send_message('msg: dispatch_failure', callback_data)
        d.addCallbacks(job_id_handled.callback, job_id_handled.errback)
        return d

    def on_job_ended_success(self, result, callback_data=None,
            job_ended_handled=None):
        d = self.send_message('msg: execute_success', callback_data)
        d.addCallbacks(job_ended_handled.callback, job_ended_handled.errback)
        return d

    def on_job_ended_failure(self, error, callback_data=None,
            job_ended_handled=None):
        token_data = {}  # XXX exit code and signal number

        d = self.send_message('msg: execute_failure', callback_data,
                token_data=token_data)
        d.addCallbacks(job_ended_handled.callback, job_ended_handled.errback)

        return d

    def send_message(self, place_name, callback_data, token_data=None):
        net_key = callback_data['net_key']
        color = callback_data['color']
        color_group_idx = callback_data['color_group_idx']
        place_idx = callback_data[place_name]

        return self.service_interfaces['orchestrator'].create_token(
                net_key=net_key, place_idx=place_idx, color=color,
                color_group_idx=color_group_idx, data=token_data)
