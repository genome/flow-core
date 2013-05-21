import flow.petri_net.transitions.barrier as barrier
import flow.redisom as rom
from flow.petri_net.net import Net, Token

from test_helpers import NetTest
from unittest import main


class TestBarrier(NetTest):
    def setUp(self):
        NetTest.setUp(self)
        self.trans = barrier.BarrierTransition.create(self.conn)
        self.color_marking = rom.Hash(connection=self.conn, key="cm")
        self.group_marking = rom.Hash(connection=self.conn, key="gm")

    def test_consume_tokens_with_empty_marking(self):
        color_group = self.net.add_color_group(size=5)

        enabler = 2
        self.trans.arcs_in = range(10)

        rv = self.trans.consume_tokens(enabler, color_group,
                self.color_marking.key, self.group_marking.key)

        self.assertEqual(5, rv)

        self.assertEqual(0, len(self.trans.enablers))
        self.assertEqual(0, len(self.trans.active_tokens(0).value))
        self.assertEqual(0, len(self.color_marking))
        self.assertEqual(0, len(self.group_marking))

    def test_consume_tokens_partially_ready(self):
        color_group = self.net.add_color_group(size=5)
        self.trans.arcs_in = range(3)

        tokens = self._make_colored_tokens(color_group)
        num_places = len(self.trans.arcs_in)

        num_successes = 0
        for i in self.trans.arcs_in:
            enabler = int(i)
            place_ids = range(enabler+1)
            for j in xrange(len(color_group.colors)):
                colors = color_group.colors[:j+1]

                self._put_tokens(place_ids, colors, color_group.idx, tokens)
                color_marking_copy = self.color_marking.value
                group_marking_copy = self.group_marking.value

                rv = self.trans.consume_tokens(enabler, color_group,
                        self.color_marking.key, self.group_marking.key)

                if rv != 0:
                    self.assertEqual(color_marking_copy,
                            self.color_marking.value)
                    self.assertEqual(group_marking_copy,
                            self.group_marking.value)
                    self.assertEqual(0, len(self.trans.enablers))
                else:
                    num_successes += 1
                    self.assertEqual(0, len(self.color_marking.value))
                    self.assertEqual(0, len(self.group_marking.value))
                    self.assertEqual(enabler, int(self.trans.enablers[color_group.idx]))
                    expected_token_keys = sorted([x.key for x in tokens.values()] *
                            num_places)

                    self.assertEqual(expected_token_keys,
                            sorted(self.trans.active_tokens(color_group.idx)))

        self.assertEqual(1, num_successes)


if __name__ == "__main__":
    main()
