from otri.filtering.filters.average_filter import AverageFilter
from otri.filtering.stream import Stream
import unittest


class AverageFilterTest(unittest.TestCase):

    def test_one_input(self):
        avg_filter = AverageFilter(Stream(), {})
        self.assertEqual(len(avg_filter.get_input_streams()), 1)

    def test_one_output(self):
        avg_filter = AverageFilter(Stream(), {})
        self.assertEqual(len(avg_filter.get_output_streams()), 1)

    def test_simple_stream_count(self):
        # Checking the counting works
        example_stream = Stream(
            [{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}, {"a": 5}, {"b": 6}, {"c": 7}]
        )
        example_stream.close()
        expected = 5
        avg_filter = AverageFilter(example_stream, ("a"))
        while len(example_stream):
            avg_filter.execute()
        self.assertEqual(avg_filter.get_counts()["a"], expected)

    def test_simple_stream_sum(self):
        # Checking the sum works
        example_stream = Stream(
            [{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}, {"a": 5}, {"b": 6}, {"c": 7}]
        )
        example_stream.close()
        expected = 15
        avg_filter = AverageFilter(example_stream, ("a"))
        while len(example_stream):
            avg_filter.execute()
        self.assertEqual(avg_filter.get_sums()["a"], expected)

    def test_simple_stream_avg(self):
        # Checking the avg works
        example_stream = Stream(
            [{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}, {"a": 5}, {"b": 6}, {"c": 7}]
        )
        example_stream.close()
        expected = 3
        avg_filter = AverageFilter(example_stream, ("a"))
        while len(example_stream):
            avg_filter.execute()
        self.assertEqual(avg_filter.get_avgs()["a"], expected)
