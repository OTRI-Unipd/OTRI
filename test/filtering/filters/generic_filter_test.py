import unittest
from otri.filtering.filters.generic_filter import GenericFilter, MultipleGenericFiler
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
        self.s_A = Stream(list(range(100)), closed=True)
        self.gen_filter.setup([self.s_A], [self.s_B], None)
        while not self.s_B.is_closed():
            self.gen_filter.execute()
        self.assertEqual(self.s_B, expected)


def MULTIPLE_EXAMPLE_OP(x):
    s = 0
    for el in x:
        s += el
    return s


class MultipleGenericFilterTest(unittest.TestCase):

    def setUp(self):
        self.gen_filter = MultipleGenericFiler(inputs=["A", "B"], outputs="C", operation=MULTIPLE_EXAMPLE_OP)

    def test_simple_stream_applies(self):
        expected = [3, 5, 7, 9]
        a = Stream([1, 2, 3, 4], closed=True)
        b = Stream([2, 3, 4, 5], closed=True)
        c = Stream()
        self.gen_filter.setup([a, b], [c], None)
        while not c.is_closed():
            self.gen_filter.execute()
        self.assertEqual(expected, c)
