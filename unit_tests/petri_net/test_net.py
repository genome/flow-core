from flow.petri_net.net import Net, ColorGroup
from test_helpers.fakeredistest import FakeRedisTest

from unittest import main

class TestNet(FakeRedisTest):
    def setUp(self):
        FakeRedisTest.setUp(self)
        self.net = Net.create(connection=self.conn)

    def test_set_initial_color(self):
        net = self.net
        net.set_initial_color(42)
        color_group = net.add_color_group(1)
        self.assertEqual(color_group.begin, 42)

    def test_set_initial_color_raises(self):
        net = self.net
        color_group = net.add_color_group(1)
        with self.assertRaises(ValueError):
            net.set_initial_color(42)

    def test_color_group(self):
        net = self.net
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
