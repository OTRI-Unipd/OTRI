from datetime import datetime, time, timezone
import unittest
import otri.utils.time_handler as th

string_datetime = "2020-04-21 08:05:20.030"
actual_datetime = datetime(year=2020, month=4, day=21,
                           hour=8, minute=5, second=20, microsecond=30000, tzinfo=timezone.utc)
epoch_datetime = 1587456320
time_datetime = time(hour=8, minute=5, second=20, microsecond=30000, tzinfo=timezone.utc)


class StringToDatetimeTest(unittest.TestCase):

    def test_string_to_datetime(self):
        self.assertEqual(actual_datetime, th.str_to_datetime(string_datetime))

    def test_datetime_to_string(self):
        self.assertEqual(string_datetime, th.datetime_to_str(actual_datetime))

    def test_datetime_to_epoch(self):
        self.assertEqual(epoch_datetime, th.datetime_to_epoch(actual_datetime))

    def test_epoch_to_datetime(self):
        self.assertEqual(actual_datetime.replace(microsecond=0), th.epoch_to_datetime(epoch_datetime))

    def test_time_equals_str_to_datetime_time(self):
        self.assertEqual(time_datetime, th.str_to_datetime(string_datetime).timetz())
