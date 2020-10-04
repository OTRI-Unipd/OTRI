import unittest
from otri.filtering.filters.threshold_filter import ThresholdFilter
from otri.filtering.stream import LocalStream

STREAM = [
    {"high": 1.00},
    {"high": 0.80},
    {"high": 0.70},
    {"high": -0.10},
    {"high": -0.30},
    {"high": 0.20},
    {"high": 0.10},
    {"high": -0.02},
]

EXPECTED_01 = {
    'high': {
        '0.0': 2,
        '0.1': 2,
        '0.2': 2,
        '0.3': 1,
        '0.4': 1,
        '0.5': 1,
        '0.6': 1,
        '0.7': 1,
        '0.8': 1,
        '0.9': 1,
        '1.0': 1,
        '-0.0': 2,
        '-0.1': 1,
        '-0.2': 1,
        '-0.3': 1
    }
}

EXPECTED_05 = {
    'high': {
        '0.0': 2,
        '0.5': 1,
        '1.0': 1,
        '-0.0': 2,
    }
}


class ThresholdFilterTest(unittest.TestCase):

    def test_step_01(self):
        f = ThresholdFilter(inputs="A", outputs="B", price_keys=['high'], step=lambda i: round(i*0.1, ndigits=3))
        a = LocalStream(STREAM, closed=True)
        b = LocalStream()
        state = dict()
        f.setup([a], [b], state)
        while not b.is_closed():
            f.execute()
        self.assertEqual(state['thresholds'], EXPECTED_01)

    def test_step_05(self):
        f = ThresholdFilter(inputs="A", outputs="B", price_keys=['high'], step=lambda i: round(i*0.5, ndigits=3))
        a = LocalStream(STREAM, closed=True)
        b = LocalStream()
        state = dict()
        f.setup([a], [b], state)
        while not b.is_closed():
            f.execute()
        self.assertEqual(state['thresholds'], EXPECTED_05)
