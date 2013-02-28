class ExecutorBase(object):
    def __init__(self, wrapper=None, default_environment={},
            mandatory_environment={}):

        self.wrapper = wrapper
        self.default_environment = default_environment
        self.mandatory_environment = mandatory_environment

    def _make_command_line(self, command_line, net_key=None,
            response_places=None, with_inputs=None, with_outputs=None):

        cmdline = self.wrapper + [
            '-n', net_key,
            '-r', response_places['begin_execute'],
            '-s', response_places['execute_success'],
            '-f', response_places['execute_failure'],
        ]

        if with_inputs:
            cmdline += ["--with-inputs", with_inputs]

        if with_outputs:
            cmdline.append("--with-outputs")

        cmdline.append('--')
        cmdline += command_line

        return [str(x) for x in cmdline]

