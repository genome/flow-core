from flow.petri_net.net import Net, ColorGroup, Token

from test_helpers import NetTest
from unittest import main

class TestNet(NetTest):
    def setUp(self):
        NetTest.setUp(self)

    def test_put_token(self):
        self.assertEqual(0, len(self.net.color_marking))
        self.assertEqual(0, len(self.net.group_marking))

        color_group = self.net.add_color_group(size=1)
        color = color_group.begin
        token = self.net.create_token(color=color,
                color_group_idx=color_group.idx)

        place_idx = 0
        color_key = "%s:%d" % (color, place_idx)
        group_key = "%s:%d" % (color_group.idx, place_idx)

        rv = self.net._put_token(place_idx, token)

        self.assertEqual(0, rv)
        self.assertEqual({color_key: token.key}, self.net.color_marking.value)
        self.assertEqual({group_key: "1"}, self.net.group_marking.value)

        # make sure putting the same token is idempotent
        rv = self.net._put_token(place_idx, token)
        self.assertEqual(0, rv)
        self.assertEqual({color_key: token.key}, self.net.color_marking.value)
        self.assertEqual({group_key: "1"}, self.net.group_marking.value)

        # make sure putting a new token is an error
        new_token = self.net.create_token(color=color,
                color_group_idx=color_group.idx)

        rv = self.net._put_token(place_idx, new_token)
        self.assertEqual(-1, rv)
        self.assertEqual({color_key: token.key}, self.net.color_marking.value)
        self.assertEqual({group_key: "1"}, self.net.group_marking.value)


if __name__ == "__main__":
    main()
