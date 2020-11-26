import unittest
from otri.filtering.filters.group_filter import TimeseriesGroupFilter
from otri.filtering.queue import LocalQueue
from datetime import timedelta


STREAM = [
    {"datetime": "2020-08-10 08:00:00.000", "open": "3.0", "close": "5.0", "high": "8.0", "low": "3.0"},
    {"datetime": "2020-08-10 08:01:00.000", "open": "2.0", "close": "6.0", "high": "9.0", "low": "2.0"},
    {"datetime": "2020-08-10 09:45:00.000", "open": "1.0", "close": "5.0", "high": "5.0", "low": "0.5"},
    {"datetime": "2020-08-11 09:03:00.000", "open": "1.0", "close": "1.0", "high": "2.0", "low": "1.0"}
]

EXPECTED_1_DAY = [
    {"datetime": "2020-08-10 00:00:00.000", "open": "3.0", "close": "5.0", "high": "9.0", "low": "0.5", "volume": "0"},
    {"datetime": "2020-08-11 00:00:00.000", "open": "1.0", "close": "1.0", "high": "2.0", "low": "1.0", "volume": "0"}
]

EXPECTED_1_MIN = [
    {"datetime": "2020-08-10 08:00:00.000", "open": "3.0", "close": "5.0", "high": "8.0", "low": "3.0", "volume": "0"},
    {"datetime": "2020-08-10 08:01:00.000", "open": "2.0", "close": "6.0", "high": "9.0", "low": "2.0", "volume": "0"},
    {"datetime": "2020-08-10 09:45:00.000", "open": "1.0", "close": "5.0", "high": "5.0", "low": "0.5", "volume": "0"},
    {"datetime": "2020-08-11 09:03:00.000", "open": "1.0", "close": "1.0", "high": "2.0", "low": "1.0", "volume": "0"}
]

EXPECTED_2_MIN = [
    {"datetime": "2020-08-10 08:00:00.000", "open": "3.0", "close": "6.0", "high": "9.0", "low": "2.0", "volume": "0"},
    {"datetime": "2020-08-10 09:44:00.000", "open": "1.0", "close": "5.0", "high": "5.0", "low": "0.5", "volume": "0"},
    {"datetime": "2020-08-11 09:02:00.000", "open": "1.0", "close": "1.0", "high": "2.0", "low": "1.0", "volume": "0"}
]

EXPECTED_5_MIN = [
    {"datetime": "2020-08-10 08:00:00.000", "open": "3.0", "close": "6.0", "high": "9.0", "low": "2.0", "volume": "0"},
    {"datetime": "2020-08-10 09:45:00.000", "open": "1.0", "close": "5.0", "high": "5.0", "low": "0.5", "volume": "0"},
    {"datetime": "2020-08-11 09:00:00.000", "open": "1.0", "close": "1.0", "high": "2.0", "low": "1.0", "volume": "0"}
]

EXPECTED_1_HOUR = [
    {"datetime": "2020-08-10 08:00:00.000", "open": "3.0", "close": "6.0", "high": "9.0", "low": "2.0", "volume": "0"},
    {"datetime": "2020-08-10 09:00:00.000", "open": "1.0", "close": "5.0", "high": "5.0", "low": "0.5", "volume": "0"},
    {"datetime": "2020-08-11 09:00:00.000", "open": "1.0", "close": "1.0", "high": "2.0", "low": "1.0", "volume": "0"}
]

EXPECTED_2_HOUR = [
    {"datetime": "2020-08-10 08:00:00.000", "open": "3.0", "close": "5.0", "high": "9.0", "low": "0.5", "volume": "0"},
    {"datetime": "2020-08-11 08:00:00.000", "open": "1.0", "close": "1.0", "high": "2.0", "low": "1.0", "volume": "0"}
]


class TimeseriesGroupFilterTest(unittest.TestCase):

    def test_group_day(self):
        self.__test(timedelta(days=1), EXPECTED_1_DAY)

    def test_group_1_min(self):
        self.__test(timedelta(minutes=1), EXPECTED_1_MIN)

    def test_group_2_min(self):
        self.__test(timedelta(minutes=2), EXPECTED_2_MIN)

    def test_group_5_min(self):
        self.__test(timedelta(minutes=5), EXPECTED_5_MIN)

    def test_group_1_hour(self):
        self.__test(timedelta(hours=1), EXPECTED_1_HOUR)

    def test_group_2_hour(self):
        self.__test(timedelta(hours=2), EXPECTED_2_HOUR)

    def __test(self, resolution: timedelta, expected: dict):
        group_filter = TimeseriesGroupFilter(inputs="A", outputs="B", target_resolution=resolution, datetime_key="datetime")
        a = LocalQueue(STREAM, closed=True)
        b = LocalQueue()
        group_filter.setup([a], [b], None)
        while not b.is_closed():
            group_filter.execute()
        self.assertEqual(b, LocalQueue(expected))
