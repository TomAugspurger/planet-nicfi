import unittest

import stactools.planet_nicfi


class TestModule(unittest.TestCase):

    def test_version(self):
        self.assertIsNotNone(stactools.planet_nicfi.__version__)
