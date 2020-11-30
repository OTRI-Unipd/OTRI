from otri.filtering.filters.summary_filter import SummaryFilter
from otri.filtering.stream import LocalStream
import unittest

ATOMS = [
    {
        "open": 1.,
        "close": -2.,
        "ticker": "Roberto"
    },
    {
        "open": 2.,
        "close": 3.,
        "ticker": "Roberto"
    },
    {
        "open": 3.,
        "ticker": "Ignazio"
    },
    {
        "open": 4.,
        "close": "8.00",
        "ticker": "Luigi"
    },
    {
        "open": 5.,
        "ticker": "8.00",
        "close": "Gianfranco"
    },
    {
        "open": 6.,
        "close": "Gianfranco",
        "ticker": "Luigi"
    },
    {
        "open": 10.,
        "close": "Genoveffo"
    }
]

EXPECTED_OPEN = [1., 10.]
EXPECTED_OPEN_COUNT = 7
EXPECTED_CLOSE = [-2., 8.]
EXPECTED_CLOSE_STRINGS = {"Gianfranco", "Genoveffo"}
EXPECTED_TICKER = {"Roberto", "Ignazio", "Luigi"}
EXPECTED_TICKER_VALUES = [8., 8.]


class SummaryFilterTest(unittest.TestCase):

    def setUp(self):
        self.f = SummaryFilter(
            inputs="in",
            outputs="out",
            state_name="Stats"
        )
        self.input = LocalStream(ATOMS, closed=True)
        self.output = LocalStream()
        self.state = dict()
        self.f.setup([self.input], [self.output], self.state)

    def test_summary(self):
        while self.input.has_next():
            self.f.execute()
        self.assertEqual(self.state["Stats"]["open"]['range'], EXPECTED_OPEN)
        self.assertEqual(self.state["Stats"]["open"]['count'], EXPECTED_OPEN_COUNT)
        self.assertEqual(self.state["Stats"]["close"]['range'], EXPECTED_CLOSE)
        self.assertEqual(self.state["Stats"]["close"]['strings'], EXPECTED_CLOSE_STRINGS)
        self.assertEqual(self.state["Stats"]["ticker"]['strings'], EXPECTED_TICKER)
        self.assertEqual(self.state["Stats"]["ticker"]['range'], EXPECTED_TICKER_VALUES)
