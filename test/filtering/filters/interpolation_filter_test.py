import unittest
from otri.filtering.queue import LocalQueue
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
        self.inputs = [LocalQueue(ATOMS, closed=True)]
        self.outputs = [LocalQueue()]
        self.f = IntradayInterpolationFilter("in", "out", interp_keys=["close"], target_gap_seconds=60)
        self.f.setup(self.inputs, self.outputs, None)

    def test_single_exec_no_output(self):
        # Assert no output is
        self.f.execute()
        self.assertFalse(self.f._has_outputted)

    def test_starts_from_moring(self):
        # First atom should be at the starting working hour
        self.f.execute()
        self.f.execute()
        self.assertEqual("2020-04-28 08:00:00.000", self.outputs[0].read()['datetime'])

    def test_stops_at_night(self):
        # Last atom is at the end of the day
        self.f.execute()
        self.f.execute()
        s_list = list()
        output_queue = self.outputs[0]
        while output_queue.has_next():
            s_list.append(output_queue.pop())
        self.assertEqual("2020-04-28 20:00:00.000", s_list[len(s_list) - 1]['datetime'])

    def test_avoid_night(self):
        self.f.execute()
        self.f.execute()
        # Having another atom later at night should avoid outputting other atoms
        self.f.execute()
        self.assertFalse(self.f._has_outputted)

    def test_avoid_early_morning(self):
        self.f.execute()
        self.f.execute()
        self.f.execute()
        self.f.execute()
        self.assertFalse(self.f._has_outputted)
