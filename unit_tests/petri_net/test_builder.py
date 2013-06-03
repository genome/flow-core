from flow.petri_net import builder
from flow.petri_net import future
from mock import Mock
from test_helpers.builder_test_base import BuilderTestBase
from test_helpers.fakeredistest import FakeRedisTest
from unittest import TestCase, main


class TestBuilderUnitTests(BuilderTestBase, FakeRedisTest):
    def setUp(self):
        FakeRedisTest.setUp(self)
        BuilderTestBase.setUp(self)


class TestBuilderNoConnection(TestCase):
    def test_init(self):
        c = Mock()
        b = builder.Builder(c)
        self.assertEqual(c, b.connection)


    def test_gather_nodes(self):
        skynet = future.FutureNet()
        fishnet = skynet.add_subnet(future.FutureNet)
        stuxnet = fishnet.add_subnet(future.FutureNet)

        for x in xrange(3):
            skynet.add_place()
            fishnet.add_place()
            stuxnet.add_place()

        for x in xrange(3):
            skynet.add_basic_transition()
            fishnet.add_basic_transition()
            stuxnet.add_basic_transition()

        expected_places = skynet.places | fishnet.places | stuxnet.places

        expected_transitions = (skynet.transitions
                | fishnet.transitions | stuxnet.transitions)

        places = {}
        transitions = {}
        builder.gather_nodes(skynet, places, transitions)

        self.assertItemsEqual(expected_places, places.keys())
        self.assertItemsEqual(expected_transitions, transitions.keys())


    def test_convert_action_args(self):
        target = Mock()
        source = Mock()
        fixed = Mock()

        orig_args = {
            'sub': source,
            'nosub': fixed
        }

        substitutions = {source: target}

        args = builder.convert_action_args(orig_args, substitutions)
        expected_args = {
                'sub': target,
                'nosub': fixed
        }

        self.assertItemsEqual(expected_args, args)


if __name__ == "__main__":
    main()
