from otri.filtering.filters.phase_filter import PhaseFilter, PhaseDeltaFilter, PhaseMulFilter
from otri.filtering.queue import LocalQueue
import unittest

EXAMPLE_ATOMS = [
    {"a": 1, "b": -1}, {"a": 2, "b": -2},
    {"a": 3, "b": -3}, {"a": 4, "b": -4},
    {"a": 5, "b": -5}, {"a": 6, "b": -6}
]
EXAMPLE_DISTANCE = 3
def ex_sum(x, y): return x + y


SUM_EXP = LocalQueue([{"a": 5}, {"a": 7}, {"a": 9}])
MUL_EXP = LocalQueue([{"a": 4}, {"a": 10}, {"a": 18}])
DEL_EXP = LocalQueue([{"a": -3}, {"a": -3}, {"a": -3}])


class PhaseFilterTest(unittest.TestCase):

    def setUp(self):
        self.input = LocalQueue(EXAMPLE_ATOMS, closed=True)
        self.output = LocalQueue()
        self.phase_filter = PhaseFilter(
            inputs="in",
            outputs="out",
            keys_to_change={"a": ex_sum},
            distance=EXAMPLE_DISTANCE
        )

    def test_empty_queue(self):
        # Testing a single execute call on an empty input Queue closes the output as well
        empty_closed_queue = LocalQueue(closed=True)
        self.phase_filter.setup([empty_closed_queue], [self.output], None)
        self.phase_filter.execute()
        self.assertTrue(self.output.is_closed())

    def test_call_after_closing(self):
        # Testing a single execute call on an empty input Queue closes the output as well
        empty_closed_queue = LocalQueue(closed=True)
        self.phase_filter.setup([empty_closed_queue], [self.output], None)
        self.phase_filter.execute()
        # execute again, no error should arise
        self.phase_filter.execute()
        self.assertTrue(self.output.is_closed())

    def test_simple_queue_applies(self):
        # Testing the method is applied to all the elements of the input queue
        expected = SUM_EXP
        self.phase_filter.setup([self.input], [self.output], None)
        while not self.output.is_closed():
            self.phase_filter.execute()
        self.assertEqual(self.output, expected)


class PhaseMulFilterTest(unittest.TestCase):

    def test_simple_queue_applies(self):
        # Testing the method is applied to all the elements of the input queue
        source_queue = LocalQueue(EXAMPLE_ATOMS, closed=True)
        output_queue = LocalQueue()
        expected = MUL_EXP
        phase_filter = PhaseMulFilter(
            inputs="in",
            outputs="out",
            keys_to_change=["a"],
            distance=EXAMPLE_DISTANCE
        )
        phase_filter.setup([source_queue], [output_queue], None)
        while not output_queue.is_closed():
            phase_filter.execute()
        self.assertEqual(output_queue, expected)


class PhaseDeltaFilterTest(unittest.TestCase):

    def test_simple_queue_applies(self):
        # Testing the method is applied to all the elements of the input queue
        source_queue = LocalQueue(EXAMPLE_ATOMS, closed=True)
        output_queue = LocalQueue()
        expected = DEL_EXP
        phase_filter = PhaseDeltaFilter(
            inputs="in",
            outputs="out",
            keys_to_change=["a"],
            distance=EXAMPLE_DISTANCE
        )
        phase_filter.setup([source_queue], [output_queue], None)
        while not output_queue.is_closed():
            phase_filter.execute()
        self.assertEqual(output_queue, expected)
