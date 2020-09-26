import unittest
from otri.filtering.stream import Stream
from otri.filtering.filters.interpolation_filter import IntradayInterpolationFilter

ATOMS = [
    {
        "ticker": "AAPL",
        "close": "100",
        "datetime": "2020-04-28 19:58:00.000"
    },
    {
        "ticker": "AAPL",
        "close": "110",
        "datetime": "2020-04-28 20:05:00.000"
    },
    {
        "ticker": "AAPL",
        "close": "120",
        "datetime": "2020-04-28 21:00:00.000"
    },
    {
        "ticker": "AAPL",
        "close": "125",
        "datetime": "2020-04-28 06:50:00.000"
    },
    {
        "ticker": "AAPL",
        "close": "130",
        "datetime": "2020-04-29 08:01:00.000"
    }
]


class IntradayInterpolationFilterTest(unittest.TestCase):

    def setUp(self):
        self.inputs = [Stream(ATOMS, closed=True)]
        self.outputs = [Stream()]
        self.f = IntradayInterpolationFilter("in", "out", interp_keys=["close"], target_gap_seconds=60)
        self.f.setup(self.inputs, self.outputs, None)

    def test_single_exec_no_output(self):
        # Assert no output is
        self.f.execute()
        self.assertEqual(0, len(self.outputs[0]))

    def test_starts_from_moring(self):
        # First atom should be at the starting working hour
        self.f.execute()
        self.f.execute()
        self.assertEqual("2020-04-28 08:00:00.000", self.outputs[0][0]['datetime'])

    def test_stops_at_night(self):
        # Last atom is at the end of the day
        self.f.execute()
        self.f.execute()
        self.assertEqual("2020-04-28 20:00:00.000", self.outputs[0][len(self.outputs[0]) - 1]['datetime'])

    def test_avoid_night(self):
        self.f.execute()
        self.f.execute()
        before_len = len(self.outputs[0])
        # Having another atom later at night should avoid outputting other atoms
        self.f.execute()
        after_len = len(self.outputs[0])
        self.assertEqual(before_len, after_len)

    def test_avoid_early_morning(self):
        self.f.execute()
        self.f.execute()
        self.f.execute()
        before_len = len(self.outputs[0])
        self.f.execute()
        after_len = len(self.outputs[0])
        self.assertEqual(before_len, after_len)
