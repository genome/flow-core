from test_helpers.builder_test_base import BuilderTestBase
from test_helpers.redistest import RedisTest
from unittest import TestCase, main


class TestBuilderSystemTests(BuilderTestBase, RedisTest):
    def setUp(self):
        RedisTest.setUp(self)
        BuilderTestBase.setUp(self)


if __name__ == "__main__":
    main()
