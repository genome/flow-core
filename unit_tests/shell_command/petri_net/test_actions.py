from flow.shell_command.petri_net import actions

import fakeredis
import mock
import unittest


class ShellCommandDispatchActionTest(unittest.TestCase):
    def setUp(self):
        self.net_constants = ['group_id', 'groups', 'umask', 'user_id', 'working_directory']

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
        self.net = mock.Mock()

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
        color_descriptor = mock.Mock()
        response_places = mock.Mock()
        token_data = {}
        executor_data = self.action.executor_data(self.net,
                color_descriptor, token_data, response_places)
        self.assertItemsEqual(self.net_constants + ['environment'],
                executor_data.keys())

    def test_set_environment(self):
        env = mock.Mock()
        color_descriptor = mock.Mock()
        self.net.constant.return_value = env

        executor_data = {}
        self.action._set_environment(self.net, color_descriptor, executor_data)

        self.assertEqual(executor_data['environment'], env)
        self.net.constant.assert_called_once_with('environment', {})

    def test_set_constants(self):
        executor_data = {}
        self.action._set_constants(self.net, executor_data)
        for c in self.net_constants:
            self.net.constant.assert_any_call(c)
            self.assertEqual(executor_data[c], self.net.constant.return_value)

        self.assertEqual(len(self.net_constants),
                len(self.net.constant.mock_calls))

    def test_set_io_files_unset(self):
        executor_data = {}
        self.action.set_io_files(self.net, executor_data, token_data={})
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
        self.action.set_io_files(self.net, executor_data, token_data={})
        self.assertEqual(expected_iofiles, executor_data)


    def test_execute(self):
        service_name = 'myservice'
        self.action.service_name = service_name

        constant_calls = []
        def constant(key, default=None):
            constant_calls.append((key, default))
            if key == 'groups':
                return [1,2,3]
            else:
                return 0

        self.net.constant = constant

        color_descriptor = mock.Mock()
        active_tokens = mock.Mock()
        service_interfaces = mock.MagicMock()

        token = mock.Mock()
        deferred = mock.Mock()
        basic_merge_action = mock.Mock()
        basic_merge_action.execute.return_value = ([token], deferred)
        with mock.patch('flow.shell_command.petri_net.actions.BasicMergeAction',
                new=basic_merge_action):
            self.action.execute(self.net, color_descriptor,
                    active_tokens, service_interfaces)

        self.assertIn(('user_id', None), constant_calls)
        self.assertIn(('group_id', None), constant_calls)
        self.assertIn(('groups', None), constant_calls)
        self.assertIn(('umask', None), constant_calls)
        self.assertIn(('working_directory', '/tmp'), constant_calls)

        basic_merge_action.execute.assert_called_once_with(self.action,
                self.net, color_descriptor, active_tokens, service_interfaces)
        deferred.addCallback.assert_called_once_with(mock.ANY)


    def test_fork_dispatch_service_name(self):
        self.assertEqual('fork', actions.ForkDispatchAction.service_name)

    def test_lsf_dispatch_service_name(self):
        self.assertEqual('lsf', actions.LSFDispatchAction.service_name)


class LSFDispatchActionTest(unittest.TestCase):
    def setUp(self):
        self.net_constants = ['group_id', 'user_id', 'groups', 'umask', 'working_directory']

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
        lsf_options = {'queue': 'long'}
        self.action.args['lsf_options'] = lsf_options

        net = mock.Mock()
        color_descriptor = mock.Mock()
        token_data = {}
        response_places = mock.Mock()
        self.action.callback_data = mock.Mock()
        self.action.callback_data.return_value = {}
        executor_data = self.action.executor_data(net,
                color_descriptor, token_data, response_places)
        self.assertItemsEqual(self.net_constants + ['environment',
            'lsf_options'], executor_data.keys())

        self.assertEqual(lsf_options, executor_data['lsf_options'])


if '__main__' == __name__:
    unittest.main()
