import flow.petri_net.transitions.barrier as barrier
import flow.redisom as rom
from flow.petri_net.net import Net, Token

from test_helpers import RedisTest, FakeOrchestrator
from unittest import main

class TestBase(RedisTest):
    def setUp(self):
        RedisTest.setUp(self)
        orch = FakeOrchestrator(self.conn)
        self.service_interfaces = orch.service_interfaces
        self.net = Net.create(self.conn, key="net")

class TestBarrier(TestBase):
    def setUp(self):
        TestBase.setUp(self)
        self.trans = barrier.BarrierTransition.create(self.conn)
        self.color_marking = rom.Hash(connection=self.conn, key="cm")
        self.group_marking = rom.Hash(connection=self.conn, key="gm")

    def test_consume_tokens_success(self):
        color_group = self.net.add_color_group(size=1)

        token = self.net.create_token(color=color_group.begin,
                color_group_idx=color_group.idx)

        notifying_place = 0
        self.trans.arcs_in = range(5)

        for place_id in self.trans.arcs_in.value:
            self.color_marking["0:%s" % place_id] = token.key
            self.group_marking["0:%s" % place_id] = 1

        self.trans.consume_tokens(notifying_place, color_group,
                self.color_marking.key, self.group_marking.key)

        self.assertEqual([token.key]*5, self.trans.active_tokens(0).value)
        self.assertEqual(0, len(self.color_marking))
        self.assertEqual(0, len(self.group_marking))

    def test_consume_tokens_large_color_group(self):
        color_group = self.net.add_color_group(size=5)

        notifying_place = 2
        self.trans.arcs_in = range(10)

        tokens = {}
        for color_id in xrange(color_group.begin, color_group.end):
            tokens[color_id] = self.net.create_token(color=color_id,
                    color_group_idx=color_group.idx)

        for place_id in self.trans.arcs_in.value:
            for color_id in xrange(color_group.begin, color_group.end):
                key = "%s:%s" % (color_id, place_id)
                self.color_marking[key] = tokens[color_id].key

            key = "%s:%s" % (color_group.idx, place_id)
            self.group_marking[key] = color_group.size

        self.trans.consume_tokens(notifying_place, color_group,
                self.color_marking.key, self.group_marking.key)

        self.assertEqual(50, len(self.trans.active_tokens(0).value))
        expected_token_keys = sorted([x.key for x in tokens.values()] * 10)
        self.assertEqual(expected_token_keys,
                sorted(self.trans.active_tokens(0)))

        self.assertEqual(0, len(self.color_marking))
        self.assertEqual(0, len(self.group_marking))



if __name__ == "__main__":
    main()
