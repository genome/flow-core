from flow.petri_net.net import Net, ColorGroup
from test_helpers.fakeredistest import FakeRedisTest

from unittest import main

class TestNet(FakeRedisTest):
    def test_color_group(self):
        net = Net.create(connection=self.conn)
        cg = net.add_color_group(parent_color=None, parent_color_group_idx=None,
                size=2)
        self.assertIsInstance(cg, ColorGroup)
        self.assertIsNone(cg.parent_color)
        self.assertIsNone(cg.parent_color_group_idx)
        self.assertEqual(0, cg.begin)
        self.assertEqual(2, cg.end)
        self.assertEqual(2, cg.size)

        cg2 = net.add_color_group(size=3)
        self.assertIsInstance(cg2, ColorGroup)
        self.assertIsNone(cg2.parent_color)
        self.assertIsNone(cg2.parent_color_group_idx)
        self.assertEqual(2, cg2.begin)
        self.assertEqual(5, cg2.end)
        self.assertEqual(3, cg2.size)


if __name__ == "__main__":
    main()
