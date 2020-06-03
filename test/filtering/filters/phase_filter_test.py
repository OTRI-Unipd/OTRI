from otri.filtering.filters.phase_filter import *
from otri.filtering.stream import Stream
import unittest

EXAMPLE_ATOMS = [
    {"a": 1, "b": -1}, {"a": 2, "b": -2},
    {"a": 3, "b": -3}, {"a": 4, "b": -4},
    {"a": 5, "b": -5}, {"a": 6, "b": -6}
]
EXAMPLE_DISTANCE = 3
def ex_sum(x, y): return x + y
SUM_EXP = [{"a": 5}, {"a": 7}, {"a": 9}]
MUL_EXP = [{"a": 4}, {"a": 10}, {"a": 18}]
DEL_EXP = [{"a": -3}, {"a": -3}, {"a": -3}]


class PhaseFilterTest(unittest.TestCase):

    def setUp(self):
        source_stream = Stream()

    def test_empty_stream(self):
        # Testing a single execute call on an empty input Stream closes the output as well
        source_stream = Stream(is_closed=True)
        phase_filter = PhaseFilter(source_stream, {"a": ex_sum}, EXAMPLE_DISTANCE)
        phase_filter.execute()
        self.assertTrue(phase_filter.get_output_stream(0).is_closed())

    def test_call_after_closing(self):
        # Testing a single execute call on an empty input Stream closes the output as well
        source_stream = Stream(is_closed=True)
        phase_filter = PhaseFilter(source_stream, {"a": ex_sum}, EXAMPLE_DISTANCE)
        phase_filter.execute()
        # execute again, no error should arise
        phase_filter.execute()
        self.assertTrue(phase_filter.get_output_stream(0).is_closed())

    def test_simple_stream_applies(self):
        # Testing the method is applied to all the elements of the input stream
        source_stream = Stream(EXAMPLE_ATOMS)
        expected = SUM_EXP
        source_stream.close()
        phase_filter = PhaseFilter(source_stream, {"a": ex_sum}, EXAMPLE_DISTANCE)
        while not phase_filter.get_output_stream(0).is_closed():
            phase_filter.execute()
        self.assertEqual(phase_filter.get_output_stream(0), expected)


class PhaseMulFilterTest(unittest.TestCase):
    
    def test_empty_stream(self):
        # Testing a single execute call on an empty input Stream closes the output as well
        source_stream = Stream(is_closed=True)
        phase_filter = PhaseMulFilter(source_stream, {"a": ex_sum}, EXAMPLE_DISTANCE)
        phase_filter.execute()
        self.assertTrue(phase_filter.get_output_stream(0).is_closed())

    def test_call_after_closing(self):
        # Testing a single execute call on an empty input Stream closes the output as well
        source_stream = Stream(is_closed=True)
        phase_filter = PhaseMulFilter(source_stream, {"a": ex_sum}, EXAMPLE_DISTANCE)
        phase_filter.execute()
        # execute again, no error should arise
        phase_filter.execute()
        self.assertTrue(phase_filter.get_output_stream(0).is_closed())

    def test_simple_stream_applies(self):
        # Testing the method is applied to all the elements of the input stream
        source_stream = Stream(EXAMPLE_ATOMS)
        expected = MUL_EXP
        source_stream.close()
        phase_filter = PhaseMulFilter(source_stream, {"a"}, EXAMPLE_DISTANCE)
        while not phase_filter.get_output_stream(0).is_closed():
            phase_filter.execute()
        self.assertEqual(phase_filter.get_output_stream(0), expected)


class PhaseDeltaFilterTest(unittest.TestCase):

    def test_empty_stream(self):
        # Testing a single execute call on an empty input Stream closes the output as well
        source_stream = Stream(is_closed=True)
        phase_filter = PhaseDeltaFilter(source_stream, {"a": ex_sum}, EXAMPLE_DISTANCE)
        phase_filter.execute()
        self.assertTrue(phase_filter.get_output_stream(0).is_closed())

    def test_call_after_closing(self):
        # Testing a single execute call on an empty input Stream closes the output as well
        source_stream = Stream(is_closed=True)
        phase_filter = PhaseDeltaFilter(source_stream, {"a": ex_sum}, EXAMPLE_DISTANCE)
        phase_filter.execute()
        # execute again, no error should arise
        phase_filter.execute()
        self.assertTrue(phase_filter.get_output_stream(0).is_closed())

    def test_simple_stream_applies(self):
        # Testing the method is applied to all the elements of the input stream
        source_stream = Stream(EXAMPLE_ATOMS)
        expected = DEL_EXP
        source_stream.close()
        phase_filter = PhaseDeltaFilter(source_stream, {"a"}, EXAMPLE_DISTANCE)
        while not phase_filter.get_output_stream(0).is_closed():
            phase_filter.execute()
        self.assertEqual(phase_filter.get_output_stream(0), expected)
