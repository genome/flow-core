from flow import petri

# netbuilder makes the "copy net" test easier
import flow.petri.netbuilder as nb

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
        self.expected_state = set([p.tokens.key for p in self.input_places])

    def test_no_connection(self):
        self.assertRaises(TypeError, petri.Net.create, None)

    def test_consume_tokens(self):
        self.assertItemsEqual(self.expected_state, self.transition.state.value)

        token = petri.Token.create(self.conn)

        for p in self.input_places:
            p.tokens.append(token.key)

        state = set(self.expected_state)
        for i in xrange(4):
            self.net.consume_tokens(self.transition, i)
            state -= set([self.input_places[i].tokens.key])
            self.assertEqual(state, self.transition.state.value,
                    "Transition state error on iteration %d" % i)

        self.assertEqual([token.key]*4, self.transition.active_tokens.value)

    def test_consume_tokens_multi(self):
        self.assertItemsEqual(self.expected_state, self.transition.state.value)

        # Create three input and output tokens
        input_tokens = [petri.Token.create(self.conn) for x in xrange(3)]
        input_token_keys = [x.key for x in input_tokens]

        output_tokens = [petri.Token.create(self.conn) for x in xrange(3)]
        output_token_keys = [x.key for x in output_tokens]


        # each place gets 3 tokens, expect for place 3, who only gets 1
        self.input_places[0].tokens.extend(input_token_keys)
        self.input_places[1].tokens.extend(input_token_keys)
        self.input_places[2].tokens.extend(input_token_keys)
        self.input_places[3].tokens.append(input_token_keys[0])

        # let the transition try to grab all the tokens
        state = set(self.expected_state)
        for i in xrange(4):
            self.net.consume_tokens(self.transition, i)
            state -= set([self.input_places[i].tokens.key])
            self.assertEqual(state, self.transition.state.value,
                    "Transition state error on iteration %d" % i)

        # One token should have been taken from each place
        self.assertEqual(2, len(self.input_places[0].tokens))
        self.assertEqual(2, len(self.input_places[1].tokens))
        self.assertEqual(2, len(self.input_places[2].tokens))
        self.assertEqual(0, len(self.input_places[3].tokens))

        # Four tokens should be held by the transition
        self.assertEqual([input_token_keys[0]]*4,
                self.transition.active_tokens.value)

        # Let's push out a new token to the output places.
        status, remaining = self.net.push_tokens(self.transition,
                output_tokens[0].key)

        self.assertTrue(status)
        self.assertEqual(1, remaining) # Just waiting on place 3...

        # This should remove the tokens from the transition
        self.assertEqual(0, len(self.transition.active_tokens))

        # The output places should have the first output token
        for p in self.output_places:
            self.assertEqual(output_token_keys[:1], p.tokens.value)

        # After pushing, we should only be waiting for place idx 3 to get a
        # token (since the rest still have 2).
        state = set([self.input_places[3].tokens.key])
        self.assertEqual(state, self.transition.state.value)

        # Dump 2 more tokens tokens into place 3 and notify the transition
        self.input_places[3].tokens.extend(input_token_keys[1:])
        self.net.consume_tokens(self.transition, 3)

        # The transition should now be enabled and holding four tokens
        self.assertEqual(0, len(self.transition.state))
        self.assertEqual([input_token_keys[1]]*4,
                self.transition.active_tokens.value)

        # The input places should each have one token left
        for p in self.input_places:
            self.assertEqual(input_token_keys[2:], p.tokens.value)

        # Let's push out a new token to the output places. They should now have
        # the first two tokens.
        token_out = petri.Token.create(self.conn)
        status, remaining = self.net.push_tokens(self.transition,
                output_token_keys[1])
        self.assertTrue(status)
        self.assertEqual(0, remaining)

        for p in self.output_places:
            self.assertEqual(output_token_keys[:2], p.tokens.value)

        # Final token
        self.net.consume_tokens(self.transition, 3)
        self.assertEqual(0, len(self.transition.state))
        self.assertEqual([input_token_keys[2]]*4,
                self.transition.active_tokens.value)

        for p in self.input_places:
            self.assertEqual(0, len(p.tokens))

        status, remaining = self.net.push_tokens(self.transition,
                output_token_keys[2])
        self.assertTrue(status)
        self.assertEqual(4, remaining)

        self.assertEqual(self.expected_state, self.transition.state.value)
        for p in self.output_places:
            self.assertEqual(output_token_keys, p.tokens.value)


    def test_push_tokens(self):
        token = petri.Token.create(self.conn)

        for p in self.input_places:
            p.tokens.append(token.key)

        for i in xrange(4):
            self.net.consume_tokens(self.transition, i)

        status, remaining, = self.net.push_tokens(self.transition, token.key)
        self.assertTrue(status)
        self.assertEqual(4, remaining)
        self.assertEqual(0, len(self.transition.active_tokens))

        for inp in self.input_places:
            self.assertEqual(0, len(inp.tokens))

        for outp in self.output_places:
            self.assertEqual(1, len(outp.tokens))

    def test_fire_transition(self):
        tokens = [petri.Token.create(self.conn) for x in xrange(5)]
        for i in xrange(len(self.input_places)):
            for t in tokens:
                self.net.set_token(i, t.key, self.service_interfaces)

        for i in xrange(len(self.input_places)):
            self.assertEqual(0, len(self.input_places[i].tokens))

        for i in xrange(len(self.output_places)):
            self.assertEqual(5, len(self.output_places[i].tokens))

        self.assertEqual(5, self.transition.action.call_count.value)



if __name__ == "__main__":
    unittest.main()
