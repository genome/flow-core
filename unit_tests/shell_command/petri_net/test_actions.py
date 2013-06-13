from flow.shell_command.petri_net import actions

import fakeredis
import mock
import unittest


class ShellCommandDispatchActionTest(unittest.TestCase):
    def setUp(self):
        self.net_constants = ['group_id', 'user_id', 'working_directory']

        self.response_places = {
            'msg: dispatch_failure': 'dfplace',
            'msg: dispatch_success': 'dsplace',
            'msg: execute_begin': 'ebplace',
            'msg: execute_failure': 'efplace',
            'msg: execute_success': 'esplace',
        }
        self.command_line =  ['my', 'command', 'line'],

        self.args = {
            'command_line': self.command_line,
        }
        self.args.update(self.response_places)

        self.connection = fakeredis.FakeRedis()
        self.connection.flushall()

        self.key = 'test_action_key'
        self.action = actions.ShellCommandDispatchAction.create(
                self.connection, self.key, args=self.args)

    def tearDown(self):
        self.connection.flushall()


    def test_correct_net_constants(self):
        self.assertEqual(self.net_constants,
                self.action.net_constants)

    def test_correct_place_refs(self):
        expected_place_refs = self.response_places.keys()
        self.assertItemsEqual(expected_place_refs, self.action.place_refs)


    def test_response_places(self):
        self.assertEqual(self.response_places,
                self.action._response_places())

    def test_executor_data_no_extras(self):
        net = mock.Mock()
        executor_data = self.action._executor_data(net)
        self.assertItemsEqual(self.net_constants + ['environment'],
                executor_data.keys())

    def test_executor_data_resources(self):
        resources = {'limit': {'one': 2}}
        self.action.args['resources'] = resources

        net = mock.Mock()
        executor_data = self.action._executor_data(net)
        self.assertItemsEqual(self.net_constants + ['environment',
            'resources'], executor_data.keys())

        self.assertEqual(resources, executor_data['resources'])


    def test_set_environment(self):
        net = mock.Mock()
        env = mock.Mock()
        net.constant.return_value = env

        executor_data = {}
        self.action._set_environment(net, executor_data)

        self.assertEqual(executor_data['environment'], env)
        net.constant.assert_called_once_with('environment', {})

    def test_set_constants(self):
        net = mock.Mock()
        executor_data = {}
        self.action._set_constants(net, executor_data)
        for c in self.net_constants:
            net.constant.assert_any_call(c)
            self.assertEqual(executor_data[c], net.constant.return_value)

        self.assertEqual(len(self.net_constants), len(net.constant.mock_calls))

    def test_set_io_files_unset(self):
        executor_data = {}
        self.action._set_io_files(executor_data)
        self.assertEqual({}, executor_data)

    def test_set_io_files_set(self):
        expected_iofiles = {
            'stderr': '/my/stderr/path',
            'stdin': '/my/stdin/path',
            'stdout': '/my/stdout/path',
        }
        for k, v in expected_iofiles.iteritems():
            self.action.args[k] = v

        executor_data = {}
        self.action._set_io_files(executor_data)
        self.assertEqual(expected_iofiles, executor_data)


    def test_execute(self):
        service_name = 'myservice'
        self.action.service_name = service_name

        net = mock.Mock()
        net.constant.return_value = 0

        color_descriptor = mock.Mock()
        active_tokens = mock.Mock()
        service_interfaces = mock.MagicMock()

        token = mock.Mock()
        deferred = mock.Mock()
        basic_merge_action = mock.Mock()
        basic_merge_action.execute.return_value = ([token], deferred)
        with mock.patch('flow.shell_command.petri_net.actions.BasicMergeAction',
                new=basic_merge_action):
            self.action.execute(net, color_descriptor,
                    active_tokens, service_interfaces)

        net.constant.assert_any_call('user_id')
        net.constant.assert_any_call('group_id')
        net.constant.assert_any_call('working_directory')

        basic_merge_action.execute.assert_called_once_with(self.action, net,
                color_descriptor, active_tokens, service_interfaces)
        deferred.addCallback.assert_called_once_with(mock.ANY)


    def test_fork_dispatch_service_name(self):
        self.assertEqual('fork', actions.ForkDispatchAction.service_name)

    def test_lsf_dispatch_service_name(self):
        self.assertEqual('lsf', actions.LSFDispatchAction.service_name)


class LSFDispatchActionTest(unittest.TestCase):
    def setUp(self):
        self.net_constants = ['group_id', 'user_id', 'working_directory']

        self.response_places = {
            'msg: dispatch_failure': 'dfplace',
            'msg: dispatch_success': 'dsplace',
            'msg: execute_begin': 'ebplace',
            'msg: execute_failure': 'efplace',
            'msg: execute_success': 'esplace',
        }
        self.command_line =  ['my', 'command', 'line'],

        self.args = {
            'command_line': self.command_line,
        }
        self.args.update(self.response_places)

        self.connection = fakeredis.FakeRedis()
        self.connection.flushall()

        self.key = 'test_action_key'
        self.action = actions.LSFDispatchAction.create(
                self.connection, self.key, args=self.args)

    def test_executor_data_all(self):
        resources = {'limit': {'one': 2}}
        self.action.args['resources'] = resources

        lsf_options = {'queue': 'long'}
        self.action.args['lsf_options'] = lsf_options

        net = mock.Mock()
        executor_data = self.action._executor_data(net)
        self.assertItemsEqual(self.net_constants + ['environment',
            'resources', 'lsf_options'], executor_data.keys())

        self.assertEqual(resources, executor_data['resources'])
        self.assertEqual(lsf_options, executor_data['lsf_options'])


if '__main__' == __name__:
    unittest.main()
