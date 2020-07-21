import unittest
from otri.filtering.filters.generic_filter import GenericFilter
from otri.filtering.stream import Stream


def EXAMPLE_OP(x): return x + 1


class GenericFilterTest(unittest.TestCase):

    def setUp(self):
        self.s_A = Stream()
        self.s_B = Stream()
        self.gen_filter = GenericFilter(
            inputs="A", outputs="B", operation=EXAMPLE_OP)

    def test_simple_stream_applies(self):
        # Testing the method is applied to all the elements of the input stream
        expected = [EXAMPLE_OP(x) for x in range(100)]
        self.s_A = Stream(list(range(100)), is_closed=True)
        self.gen_filter.setup([self.s_A], [self.s_B], None)
        while not self.s_B.is_closed():
            self.gen_filter.execute()
        self.assertEqual(self.s_B, expected)
