from flow import petri
from twisted.internet import defer

import flow.redisom as rom

from test_helpers import RedisTest, FakeOrchestrator
import mock
import os
import pwd
import redis
import sys
import unittest


class TestBase(RedisTest):
    def setUp(self):
        RedisTest.setUp(self)
        orch = FakeOrchestrator(self.conn)
        self.service_interfaces = orch.service_interfaces


class TestNet(TestBase):
    def setUp(self):
        TestBase.setUp(self)

        input_place_names = ["input %d" % i for i in xrange(4)]
        output_place_names = ["output %d" % i for i in xrange(3)]
        action = petri.CounterAction.create(connection=self.conn, name="counter")
        place_arcs_out = {i: [0] for i in xrange(4)}
        trans_arcs_out = {0: range(4, 7)}

        self.net = petri.Net.create(
                connection=self.conn,
                place_names=input_place_names + output_place_names,
                trans_actions=[action],
                place_arcs_out=place_arcs_out,
                trans_arcs_out=trans_arcs_out)

        self.input_places = [self.net.place(x) for x in xrange(4)]
        self.output_places = [self.net.place(x) for x in xrange(4, 7)]
        self.transition = self.net.transition(0)

    @property
    def expected_state(self):
        ncol = self.net.num_token_colors.value
        rv = {}
        state = [str(x) for x in xrange(len(self.input_places))]
        for i in xrange(ncol):
            rv[i] = set(state)

        return rv

    @property
    def transition_state(self):
        ncol = self.net.num_token_colors.value
        return {x: self.transition.state(x).value for x in xrange(ncol)}

    def test_no_connection(self):
        self.assertRaises(TypeError, petri.Net.create, None)

    def test_num_token_colors(self):
        uncolored_token = self.net.create_token()
        too_large_token = self.net.create_token(token_color=99)
        valid_token = self.net.create_token(token_color=9)

        self.assertRaises(petri.TokenColorError, self.net.add_token,
                0, uncolored_token)

        self.net.set_num_token_colors(10)
        self.assertRaises(petri.TokenColorError, self.net.add_token,
                0, uncolored_token)
        self.assertRaises(petri.TokenColorError, self.net.add_token,
                0, too_large_token)

        self.net.add_token(0, valid_token)

    def test_consume_tokens(self):
        self.net.set_num_token_colors(1)

        expected_state = self.expected_state
        self.assertItemsEqual(expected_state, self.transition_state)

        token = petri.Token.create(self.conn, color_idx=0)

        for i in xrange(len(self.input_places)):
            self.net.marking(0)[i] = token.key

        expstate = expected_state[0]
        num_places = len(self.input_places)
        for i in xrange(num_places):
            self.net.consume_tokens(self.transition, i, 0)
            expstate.remove(str(i))
            self.assertEqual(expstate, self.transition.state(0).value,
                    "Transition state error on iteration %d" % i)

        self.assertEqual([token.key]*4,
                self.transition.active_tokens_for_color(0).value)

    def test_consume_tokens_multi(self):
        # Create three input and output tokens
        num_toks = 3
        self.net.set_num_token_colors(num_toks)
        input_tokens = [petri.Token.create(self.conn, color_idx=x)
                for x in xrange(num_toks)]
        input_token_keys = [x.key for x in input_tokens]

        self.assertItemsEqual(self.expected_state, self.transition_state)

        output_tokens = [petri.Token.create(self.conn)
                for x in xrange(num_toks)]
        output_token_keys = [x.key for x in output_tokens]


        # each place gets 3 tokens
        for tidx, tkey in enumerate(input_token_keys):
            for pidx, place in enumerate(self.input_places):
                self.net.marking(tidx)[pidx] = tkey

        # let the transition try to grab all the tokens
        state = self.expected_state
        for col_idx in xrange(num_toks):
            for i in xrange(4):
                self.net.consume_tokens(self.transition, i, col_idx)
                state[col_idx].discard(str(i))
                self.assertEqual(state[col_idx],
                        self.transition.state(col_idx).value,
                        "Transition state error on iteration color: %d, "
                        "place: %d" % (col_idx, i))

            # Marking for this color should be empty
            self.assertEqual(0, len(self.net.marking(col_idx)))

            # Four tokens should be held by the transition
            self.assertEqual([input_token_keys[col_idx]]*4,
                    self.transition.active_tokens_for_color(col_idx).value)

            # Let's push out a new token to the output places.
            status, arcs_out = self.net.push_tokens(self.transition,
                    output_tokens[col_idx].key, col_idx)

            self.assertTrue(status)

            # This should remove the tokens from the transition
            self.assertEqual(0,
                    len(self.transition.active_tokens_for_color(col_idx)))

            # The output places should have output token
            marks = self.net.marking(col_idx).value
            for i in xrange(len(self.output_places)):
                place_idx = i + len(self.input_places)
                self.assertIn(str(place_idx), marks)

    def test_push_tokens(self):
        self.net.set_num_token_colors(1)
        token = petri.Token.create(self.conn, color_idx=0)

        for pidx in xrange(len(self.input_places)):
            self.net.marking(0)[pidx] = token.key

        for i in xrange(4):
            self.net.consume_tokens(self.transition, i, 0)

        status, arcs_out = self.net.push_tokens(self.transition, token.key, 0)
        self.assertTrue(status)
        self.assertEqual(0, len(self.transition.active_tokens_for_color(0)))

        marking = self.net.marking(0)
        for pidx in xrange(len(self.input_places)):
            self.assertNotIn(pidx, marking)

        for pidx in xrange(len(self.output_places)):
            pidx += len(self.input_places)
            self.assertIn(pidx, marking)

    def test_fire_transition(self):
        num_toks = 5
        self.net.set_num_token_colors(num_toks)
        tokens = [petri.Token.create(self.conn, color_idx=x)
                for x in xrange(num_toks)]

        for i in xrange(len(self.input_places)):
            for t in tokens:
                self.net.add_token(i, t)
                self.net.notify_place(i, t.color_idx, self.service_interfaces)

        for pidx in xrange(len(self.input_places)):
            self.assertEqual('0', self.net.global_marking[pidx])

        for pidx in xrange(len(self.output_places)):
            pidx += len(self.input_places)
            self.assertEqual('5', self.net.global_marking[pidx])

        self.assertEqual(5, self.transition.action.call_count.value)


class TestableColorJoinAction(petri.ColorJoinAction):
    completed = rom.Property(rom.String)
    def on_complete(self, active_tokens_key, net, service_interfaces):
        self.completed = "hello"
        return defer.succeed(None)

class TestJoinAction(TestBase):
    def test_join_action(self):
        places = ["p1", "p2"]
        action = TestableColorJoinAction.create(self.conn, name="countdown")

        place_arcs_out = {0: {0}}
        trans_arcs_out = {0: {1}}

        net = petri.Net.create(
                connection=self.conn,
                place_names=places,
                trans_actions=[action],
                place_arcs_out=place_arcs_out,
                trans_arcs_out=trans_arcs_out)

        net.set_num_token_colors(4)

        for i in xrange(4):
            net.create_set_notify(0, token_color=i,
                    service_interfaces=self.service_interfaces)
            if i < 3:
                self.assertRaises(rom.NotInRedisError, getattr,
                    action.completed, "value")

        self.assertEqual("hello", action.completed.value)


if __name__ == "__main__":
    unittest.main()
