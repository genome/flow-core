import unittest
try:
    from unittest import mock
except:
    import mock

from flow.amqp_manager import delegate_base

class DelegateTest(unittest.TestCase):
    def setUp(self):
        self.ready_callback = mock.Mock()

        self.children = [delegate_base.Delegate(),
                         delegate_base.Delegate()]

        self.parent = delegate_base.Delegate(self.children)
        self.parent.add_ready_callback(self.ready_callback)

    def test_notify_ready(self):
        self.children[0].notify_ready()
        self.assertFalse(self.ready_callback.called)
        self.children[1].notify_ready()
        self.ready_callback.assert_called_once_with(self.parent)


if '__main__' == __name__:
    unittest.main()
