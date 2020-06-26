from otri.filtering.filters.statistics_filter import StatisticsFilter
from otri.filtering.stream import Stream
import unittest


class StatisticsFilterTest(unittest.TestCase):

    def test_one_input(self):
        ex_filter = StatisticsFilter(Stream(), {}).calc_avg()
        self.assertEqual(len(ex_filter.get_input_streams()), 1)

    def test_one_output(self):
        ex_filter = StatisticsFilter(Stream(), {}).calc_avg()
        self.assertEqual(len(ex_filter.get_output_streams()), 1)

    def test_empty_stream(self):
        # Testing a single execute call on an empty input Stream closes the output as well
        source_stream = Stream(is_closed=True)
        ex_filter = StatisticsFilter(source_stream, {})
        ex_filter.execute()
        self.assertTrue(ex_filter.get_output_stream(0).is_closed())

    def test_call_after_closing(self):
        # Testing a single execute call on an empty input Stream closes the output as well
        source_stream = Stream(is_closed=True)
        ex_filter = StatisticsFilter(source_stream, {})
        ex_filter.execute()
        # execute again, no error should arise
        ex_filter.execute()
        self.assertTrue(ex_filter.get_output_stream(0).is_closed())

    def test_simple_stream_count(self):
        # Checking the counting works
        example_stream = Stream(
            [{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}, {"a": 5}, {"b": 6}, {"c": 7}]
        )
        example_stream.close()
        expected = 5
        ex_filter = StatisticsFilter(example_stream, ("a")).calc_count()
        while len(example_stream):
            ex_filter.execute()
        self.assertEqual(ex_filter.get_count()["a"], expected)

    def test_simple_stream_sum(self):
        # Checking the sum works
        example_stream = Stream(
            [{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}, {"a": 5}, {"b": 6}, {"c": 7}]
        )
        example_stream.close()
        expected = 15
        ex_filter = StatisticsFilter(example_stream, ("a")).calc_sum()
        while len(example_stream):
            ex_filter.execute()
        self.assertEqual(ex_filter.get_sum()["a"], expected)

    def test_simple_stream_avg(self):
        # Checking the avg works
        example_stream = Stream(
            [{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}, {"a": 5}, {"b": 6}, {"c": 7}]
        )
        example_stream.close()
        expected = 3
        ex_filter = StatisticsFilter(example_stream, ("a")).calc_avg()
        while len(example_stream):
            ex_filter.execute()
        self.assertEqual(ex_filter.get_avg()["a"], expected)

    def test_simple_stream_max(self):
        # Checking the avg works
        example_stream = Stream(
            [{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}, {"a": 5}, {"b": 6}, {"c": 7}]
        )
        example_stream.close()
        expected = 5
        ex_filter = StatisticsFilter(example_stream, ("a")).calc_max()
        while len(example_stream):
            ex_filter.execute()
        self.assertEqual(ex_filter.get_max()["a"], expected)

    def test_simple_stream_min(self):
        # Checking the avg works
        example_stream = Stream(
            [{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}, {"a": 5}, {"b": 6}, {"c": 7}]
        )
        example_stream.close()
        expected = 1
        ex_filter = StatisticsFilter(example_stream, ("a")).calc_min()
        while len(example_stream):
            ex_filter.execute()
        self.assertEqual(ex_filter.get_min()["a"], expected)
