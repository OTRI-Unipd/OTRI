from otri.utils import config as cfg
import unittest

class ConfigTest(unittest.TestCase):

    def test_get_key_no_exception(self):
        # Should almost always return None, depends if there actually is a config.json file and if it contains the test-value.
        cfg.get_value("test-value")