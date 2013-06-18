from flow.shell_command.lsf import executor
from flow.shell_command.resource import ResourceException
from pythonlsf import lsf

import mock
import re
import unittest


class LSFExecutorInitTest(unittest.TestCase):
    def test_no_path_to_pre_exec(self):
        with self.assertRaises(RuntimeError):
            executor.LSFExecutor(pre_exec=['DOESNOTEXIST'], post_exec=['true'],
                    option_definitions={}, default_options={},
                    resource_definitions={})

    def test_no_path_to_post_exec(self):
        with self.assertRaises(RuntimeError):
            executor.LSFExecutor(pre_exec=['true'], post_exec=['DOESNOTEXIST'],
                    option_definitions={}, default_options={},
                    resource_definitions={})


class LSFExecutorSetRequestPrePost(unittest.TestCase):
    def setUp(self):
        self.executor = executor.LSFExecutor(pre_exec=['true'], post_exec=['true'],
                option_definitions={}, default_options={},
                resource_definitions={})

    def test_pre_exec(self):
        request = mock.Mock()
        request.options = 0

        executor_data = mock.Mock()

        pre_exec_string = 'exciting pre exec string'
        make_pre_post_command_string = mock.Mock()
        make_pre_post_command_string.return_value = pre_exec_string
        with mock.patch(
                'flow.shell_command.lsf.executor.make_pre_post_command_string',
                new=make_pre_post_command_string):
            self.executor.set_pre_exec(request, executor_data)

        response_places = {
                'msg: execute_begin': '--execute-begin',
        }
        make_pre_post_command_string.assert_called_once_with(
                self.executor.pre_exec_command,
                executor_data, response_places)
        self.assertEqual(pre_exec_string, request.preExecCmd)
        self.assertEqual(lsf.SUB_PRE_EXEC, request.options)

    def test_post_exec(self):
        request = mock.Mock()
        request.options3 = 0

        executor_data = mock.Mock()

        post_exec_string = 'exciting post exec string'
        make_pre_post_command_string = mock.Mock()
        make_pre_post_command_string.return_value = post_exec_string
        with mock.patch(
                'flow.shell_command.lsf.executor.make_pre_post_command_string',
                new=make_pre_post_command_string):
            self.executor.set_post_exec(request, executor_data)

        response_places = {
                'msg: execute_success': '--execute-success',
                'msg: execute_failure': '--execute-failure',
        }
        make_pre_post_command_string.assert_called_once_with(
                self.executor.post_exec_command,
                executor_data, response_places)
        self.assertEqual(post_exec_string, request.postExecCmd)
        self.assertEqual(lsf.SUB3_POST_EXEC, request.options3)



class LSFExecutorCallbackTest(unittest.TestCase):
    def setUp(self):
        self.executor = executor.LSFExecutor(pre_exec=['true'], post_exec=['true'],
                option_definitions={}, default_options={},
                resource_definitions={})

    def test_on_job_id(self):
        job_id = mock.Mock()
        callback_data = mock.Mock()
        service_interfaces = mock.Mock()

        send_message = mock.Mock()
        with mock.patch('flow.shell_command.lsf.executor.send_message',
                new=send_message):
            self.executor.on_job_id(job_id, callback_data, service_interfaces)

        send_message.assert_any_call(
                'msg: dispatch_success', callback_data, service_interfaces,
                token_data={'job_id': job_id})

    def test_on_failure(self):
        exit_code = mock.Mock()
        callback_data = mock.Mock()
        service_interfaces = mock.Mock()

        send_message = mock.Mock()
        with mock.patch('flow.shell_command.lsf.executor.send_message',
                new=send_message):
            self.executor.on_failure(exit_code,
                    callback_data, service_interfaces)

        send_message.assert_any_call(
                'msg: dispatch_failure', callback_data, service_interfaces,
                token_data={'exit_code': exit_code})



class PrePostCommandStringTest(unittest.TestCase):
    def test_simple(self):
        executor_data = {
            'net_key': '<NK>',
            'color': '<C>',
            'color_group_idx': '<G>',
        }
        response_places = { }

        expected_result = ('bash -c "\'EXE\' --net-key \'<NK>\' --color <C> '
                '--color-group-idx <G>"')
        self.assertEqual(expected_result,
                executor.make_pre_post_command_string('EXE',
                    executor_data, response_places))


    def test_outputs(self):
        executor_data = {
            'net_key': '<NK>',
            'color': '<C>',
            'color_group_idx': '<G>',
            'lsf_options': {
                'stdout': 'STDOUT',
                'stderr': 'STDERR',
            },
        }
        response_places = { }

        expected_result = ('bash -c "\'EXE\' --net-key \'<NK>\' --color <C> '
                '--color-group-idx <G> 1>> \'STDOUT\' 2>> \'STDERR\'"')
        self.assertEqual(expected_result,
                executor.make_pre_post_command_string('EXE',
                    executor_data, response_places))

    def test_response_places(self):
        executor_data = {
            'net_key': '<NK>',
            'color': '<C>',
            'color_group_idx': '<G>',
            'place_one': 1,
            'place_two': 2,
        }
        response_places = {
            'place_one': '--place-one',
            'place_two': '--place-two',
        }

        expected_result = ('bash -c "\'EXE\' --net-key \'<NK>\' --color <C> '
                '--color-group-idx <G> --place-one 1 --place-two 2"')
        self.assertEqual(expected_result,
                executor.make_pre_post_command_string('EXE',
                    executor_data, response_places))


if '__main__' == __name__:
    unittest.main()
