import os


class ExecutionEnvironment(object):
    def __init__(self, user_id, group_id, environment, working_directory):
        self.user_id = user_id
        self.group_id = group_id
        self.environment = environment
        self.working_directory = working_directory


class NullExecutionEnvironment(object):
    def __init__(self):
        self.user_id = os.getuid()
        self.group_id = os.getgid()
        self.environment = {}
        self.working_directory = '/tmp'
