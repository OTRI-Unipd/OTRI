import unittest
from otri.filtering.filters.align_filter import AlignFilter
from otri.filtering.stream import Stream

STREAMS = [
    [
        {"datetime": "2020-08-10 08:00:00.000", "A": "B"},
        {"datetime": "2020-08-10 08:01:00.000", "A": "B"},
        {"datetime": "2020-08-10 08:03:00.000", "A": "B"}
    ],
    [
        {"datetime": "2020-08-10 08:00:00.000", "C": "D"},
        {"datetime": "2020-08-10 08:02:00.000", "C": "D"},
        {"datetime": "2020-08-10 08:03:00.000", "C": "D"}
    ]
]

EXPECTED = [
    [
        {"datetime": "2020-08-10 08:00:00.000", "A": "B"},
        {"datetime": "2020-08-10 08:03:00.000", "A": "B"}
    ],
    [
        {"datetime": "2020-08-10 08:00:00.000", "C": "D"},
        {"datetime": "2020-08-10 08:03:00.000", "C": "D"}
    ]
]


class AlignFilterTest(unittest.TestCase):

    def setUp(self):
        self.align_filter = AlignFilter(inputs=["A", "B"], outputs=["C", "D"], datetime_key="datetime")

    def test_alignment(self):
        a = Stream(STREAMS[0], closed=True)
        b = Stream(STREAMS[1], closed=True)
        c = Stream()
        d = Stream()
        self.align_filter.setup([a, b], [c, d], None)
        while not c.is_closed():
            self.align_filter.execute()
        self.assertEqual(c, EXPECTED[0])
