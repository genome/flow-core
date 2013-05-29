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
                token = token_hash[color_id]
                token_key = token.index.value
                self.net.put_token(place_id, token)

    def setup_transition(self, cls, n_input_places, n_output_places):
        input_places = []
        output_places = []
        trans = self.net.add_transition(cls)
        for i in xrange(n_input_places):
            p = self.net.add_place("input place %d" % i)
            trans.arcs_in.append(p.index.value)

        for i in xrange(n_output_places):
            p = self.net.add_place("output place %d" % i)
            trans.arcs_out.append(p.index.value)

        return trans
