from flow.petri_net import builder
from flow.petri_net import future
from mock import Mock

import itertools


class BuilderTestBase(object):
    def setUp(self):
        self.builder = builder.Builder(self.conn)

        self.stored_net = Mock()
        self.test_key = 'thing_under_test'
        self.stored_net.place_key.return_value = self.test_key
        self.stored_net.transition_key.return_value = self.test_key


    def test_store(self):
        skynet = future.FutureNet('skynet')
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

        variables = {'v': '1'}
        constants = {'c': '2'}

        stored_net = self.builder.store(skynet, variables, constants)

        self.assertEqual('skynet', stored_net.name.value)
        self.assertItemsEqual(variables, stored_net.variables.value)

        for k, v in constants.iteritems():
            self.assertEqual(v, stored_net.constant(k))


    def test_create_stored_net(self):
        future_net = Mock()
        future_net.name = 'netname'

        variables = {'hi': 'there!'}
        constants = {'one': 'constant', 'two': 'fish'}
        stored_net = self.builder.create_stored_net(future_net,
                variables, constants)

        self.assertEqual('netname', stored_net.name.value)
        self.assertItemsEqual(variables, stored_net.variables.value)

        for k, v in constants.iteritems():
            self.assertEqual(v, stored_net.constant(k))


    def test_store_place(self):
        fp = future.FuturePlace('Paris')
        fp.add_arc_in(future.FutureBasicTransition())
        fp.add_arc_in(future.FutureBasicTransition())
        fp.add_arc_out(future.FutureBasicTransition())
        fp.add_arc_out(future.FutureBasicTransition())

        future_transitions = {x: i
                for i, x in enumerate(itertools.chain(fp.arcs_in, fp.arcs_out))}

        p = self.builder.store_place(self.stored_net, fp, 0, future_transitions)

        self.assertEqual(p.key, self.test_key)
        self.assertTrue(self.conn.exists(self.test_key))
        self.assertEqual(fp.name, p.name.value)

        self.assertItemsEqual([0, 1], p.arcs_in)
        self.assertItemsEqual([2, 3], p.arcs_out)

    def test_store_transitions(self):
        ft = future.FutureBasicTransition('Reading Railroad')
        ft.add_arc_in(future.FuturePlace())
        ft.add_arc_in(future.FuturePlace())
        ft.add_arc_out(future.FuturePlace())
        ft.add_arc_out(future.FuturePlace())

        future_places = {x: i
                for i, x in enumerate(itertools.chain(ft.arcs_in, ft.arcs_out))}

        t = self.builder.store_transition(self.stored_net, ft, 0, future_places)

        self.assertEqual(t.key, self.test_key)
        self.assertTrue(self.conn.exists(self.test_key))
        self.assertEqual(ft.name, t.name.value)

        self.assertItemsEqual([0, 1], t.arcs_in)
        self.assertItemsEqual([2, 3], t.arcs_out)
