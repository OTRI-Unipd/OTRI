from datetime import datetime
import unittest
import otri.utils.time_handler as th

string_datetime = "2020-04-21 08:05:20.030"
actual_datetime = datetime(year=2020, month=4, day=21,
                           hour=8, minute=5, second=20, microsecond=30000)


class StringToDatetimeTest(unittest.TestCase):

    def test_string_to_datetime(self):
        self.assertEqual(actual_datetime, th.str_to_datetime(string_datetime))

    def test_datetime_to_string(self):
        self.assertEqual(string_datetime, th.datetime_to_str(actual_datetime))
