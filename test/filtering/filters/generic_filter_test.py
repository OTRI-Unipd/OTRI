import unittest
from otri.filtering.filters.generic_filter import GenericFilter, MultipleGenericFiler
from otri.filtering.queue import LocalQueue


def EXAMPLE_OP(x): return x + 1


class GenericFilterTest(unittest.TestCase):

    def setUp(self):
        self.s_A = LocalQueue()
        self.s_B = LocalQueue()
        self.gen_filter = GenericFilter(
            inputs="A", outputs="B", operation=EXAMPLE_OP)

    def test_simple_queue_applies(self):
        # Testing the method is applied to all the elements of the input queue
        expected = LocalQueue([EXAMPLE_OP(x) for x in range(100)])
        self.s_A = LocalQueue(list(range(100)), closed=True)
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

    def test_simple_queue_applies(self):
        expected = LocalQueue([3, 5, 7, 9])
        a = LocalQueue([1, 2, 3, 4], closed=True)
        b = LocalQueue([2, 3, 4, 5], closed=True)
        c = LocalQueue()
        self.gen_filter.setup([a, b], [c], None)
        while not c.is_closed():
            self.gen_filter.execute()
        self.assertEqual(expected, c)
