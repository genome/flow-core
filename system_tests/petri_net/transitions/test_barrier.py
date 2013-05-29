from flow.petri_net.transitions.barrier import BarrierTransition
from flow.petri_net.net import Net, Token, ColorDescriptor
from flow.petri_net.transitions.action import TransitionAction
from test_helpers import NetTest
import flow.redisom as rom

from mock import MagicMock
from unittest import main

class SimpleAction(TransitionAction):
    count = rom.Property(rom.Int)

    def execute(self, color_descriptor, active_tokens_key, net,
            service_interfaces):

        self.count.incr(1)
        color = color_descriptor.color
        color_group_idx = color_descriptor.group.idx

        new_token = net.create_token(color, color_group_idx)
        return [new_token]


class TestBarrier(NetTest):
    def setUp(self):
        NetTest.setUp(self)

    def test_consume_tokens_with_empty_marking(self):
        color_group = self.net.add_color_group(size=5)
        enabler = color_group.begin + 1
        color_descriptor = ColorDescriptor(enabler, color_group)

        trans = self.setup_transition(BarrierTransition, 10, 0)

        rv = trans.consume_tokens(enabler, color_descriptor,
                self.net.color_marking.key, self.net.group_marking.key)

        self.assertEqual(5, rv)

        self.assertEqual(0, len(trans.enablers))
        self.assertEqual(0,
                len(trans.active_tokens(color_descriptor).value))
        self.assertEqual(0, len(self.net.color_marking))
        self.assertEqual(0, len(self.net.group_marking))

    def test_consume_tokens(self):
        color_group = self.net.add_color_group(size=5)
        trans = self.setup_transition(BarrierTransition, 3, 0)

        tokens = self._make_colored_tokens(color_group)

        num_successes = 0
        for i in trans.arcs_in:
            enabler = int(i)
            place_ids = range(enabler+1)
            for j in xrange(len(color_group.colors)):
                colors = color_group.colors[:j+1]
                color_descriptor = ColorDescriptor(j, color_group)

                self._put_tokens(place_ids, colors, color_group.idx, tokens)
                color_marking_copy = self.net.color_marking.value
                group_marking_copy = self.net.group_marking.value

                rv = trans.consume_tokens(enabler, color_descriptor,
                        self.net.color_marking.key, self.net.group_marking.key)

                if rv != 0:
                    self.assertEqual(color_marking_copy,
                            self.net.color_marking.value)
                    self.assertEqual(group_marking_copy,
                            self.net.group_marking.value)
                    self.assertEqual(0, len(trans.enablers))
                else:
                    num_successes += 1
                    self.assertEqual(0, len(self.net.color_marking.value))
                    self.assertEqual(0, len(self.net.group_marking.value))
                    self.assertEqual(enabler,
                            int(trans.enablers[color_group.idx]))
                    expected_token_keys = [str(x.index.value)
                            for x in tokens.values()]

                    self.assertItemsEqual(expected_token_keys,
                            trans.active_tokens(color_descriptor))

        self.assertEqual(1, num_successes)

    def test_push_tokens(self):
        color_group = self.net.add_color_group(size=1)
        color_descriptor = ColorDescriptor(color_group.begin, color_group)

        trans = self.setup_transition(BarrierTransition, 4, 2)

        tokens = self._make_colored_tokens(color_group)

        self._put_tokens(trans.arcs_in, color_group.colors,
                color_group.idx, tokens)

        rv = trans.consume_tokens(0, color_descriptor,
                self.net.color_marking.key, self.net.group_marking.key)

        self.assertEqual(0, rv)
        self.assertEqual(1,
                len(trans.active_tokens(color_descriptor).value))

        rv = trans.push_tokens(self.net, color_descriptor, tokens.values())

        expected_color = {"0:4": "0", "0:5": "0"}
        expected_group = {"0:4": "1", "0:5": "1"}

        self.assertEqual(0,
                len(trans.active_tokens(color_descriptor).value))
        self.assertEqual(expected_color, self.net.color_marking.value)
        self.assertEqual(expected_group, self.net.group_marking.value)

    def test_fire_action(self):
        color_group = self.net.add_color_group(size=1)
        color_descriptor = ColorDescriptor(color_group.begin, color_group)
        action = SimpleAction.create(self.conn)

        trans = self.setup_transition(BarrierTransition, 4, 2)
        trans.action_key = action.key

        tokens = self._make_colored_tokens(color_group)

        self._put_tokens(trans.arcs_in, color_group.colors,
                color_group.idx, tokens)

        rv = trans.consume_tokens(0, color_descriptor,
                self.net.color_marking.key, self.net.group_marking.key)

        self.assertEqual(0, rv)
        self.assertEqual(1,
                len(trans.active_tokens(color_descriptor).value))

        svcs = MagicMock()
        new_tokens = trans.fire(self.net, color_descriptor, svcs)
        self.assertEqual(1, action.count.value)
        self.assertEqual(1, len(new_tokens))
        token = new_tokens[0]
        self.assertEqual(color_descriptor.color, token.color.value)
        self.assertEqual(color_descriptor.group.idx,
                token.color_group_idx.value)


if __name__ == "__main__":
    main()
