from otri.filtering.filters.statistics_filter import StatisticsFilter
from otri.filtering.queue import LocalQueue
import unittest

ATOMS = [{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}, {"a": 5}, {"b": 6}, {"c": 7}]


class StatisticsFilterTest(unittest.TestCase):

    def setUp(self):
        self.f = StatisticsFilter(
            inputs="in",
            outputs="out",
            keys=["a", "b", "c"]
        )
        self.input = LocalQueue(ATOMS, closed=True)
        self.output = LocalQueue()
        self.state = dict()
        self.f.setup([self.input], [self.output], self.state)

    def test_simple_queue_count(self):
        # Checking the counting works
        self.f.calc_count("count")
        self.f.setup([self.input], [self.output], self.state)
        while self.input.has_next():
            self.f.execute()
        self.assertEqual(self.state["count"]["a"], 5)

    def test_simple_queue_sum(self):
        # Checking the sum works
        self.f.calc_sum("sum")
        self.f.setup([self.input], [self.output], self.state)
        while self.input.has_next():
            self.f.execute()
        self.assertEqual(self.state["sum"]["a"], 15)

    def test_simple_queue_avg(self):
        # Checking the avg works
        self.f.calc_avg("avg")
        self.f.setup([self.input], [self.output], self.state)
        while self.input.has_next():
            self.f.execute()
        self.assertEqual(self.state["avg"]["a"], 3)

    def test_simple_queue_max(self):
        # Checking the max works
        self.f.calc_max("max")
        self.f.setup([self.input], [self.output], self.state)
        while self.input.has_next():
            self.f.execute()
        self.assertEqual(self.state["max"]["a"], 5)

    def test_simple_queue_min(self):
        # Checking the min works
        self.f.calc_min("min")
        self.f.setup([self.input], [self.output], self.state)
        while self.input.has_next():
            self.f.execute()
        self.assertEqual(self.state["min"]["a"], 1)

    def test_state_duplicate_op_name(self):
        self.f.calc_count("test")
        self.f.calc_sum("test")
        self.assertRaises(ValueError, self.f.setup, [self.input], [self.output], self.state)
