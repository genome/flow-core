from redistest import RedisTest
from flow.petri_net.net import Net

class NetTest(RedisTest):
    def setUp(self):
        RedisTest.setUp(self)
        self.net = Net.create(self.conn, key="net")

    def _make_colored_tokens(self, color_group):
        tokens = {}
        for color_id in xrange(color_group.begin, color_group.end):
            tokens[color_id] = self.net.create_token(color=color_id,
                    color_group_idx=color_group.idx)
        return tokens

    def _put_tokens(self, place_ids, color_ids, cg_id, token_hash):
        for place_id in place_ids:
            for color_id in color_ids:
                token_key = token_hash[color_id].key
                key = "%s:%s" % (color_id, place_id)
                self.color_marking[key] = token_key

            key = "%s:%s" % (cg_id, place_id)
            self.group_marking[key] = len(color_ids)
