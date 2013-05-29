from flow.petri_net.transitions.basic import BasicTransition
import flow.redisom as rom
from flow.petri_net.net import Net, Token, ColorDescriptor

from test_helpers import NetTest
from unittest import main


class TestBasic(NetTest):
    def setUp(self):
        NetTest.setUp(self)

    def test_consume_tokens_with_empty_marking(self):
        color_group = self.net.add_color_group(size=5)
        color = color_group.begin
        color_descriptor = ColorDescriptor(color, color_group)

        enabler = 2
        trans = self.setup_transition(BasicTransition, 10, 0)

        rv = trans.consume_tokens(enabler, color_descriptor,
                self.net.color_marking.key, self.net.group_marking.key)

        self.assertNotEqual(0, rv)

        self.assertEqual(0, len(trans.enablers))
        self.assertEqual(0,
                len(trans.active_tokens(color_descriptor).value))
        self.assertEqual(0, len(self.net.color_marking))
        self.assertEqual(0, len(self.net.group_marking))

    def test_consume_tokens_partially_ready(self):
        color_group = self.net.add_color_group(size=1)
        trans = self.setup_transition(BasicTransition, 3, 0)

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

        trans = self.setup_transition(BasicTransition, 4, 2)

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


if __name__ == "__main__":
    main()
