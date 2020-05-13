import unittest
from otri.filtering.filters.generic_filter import GenericFilter
from otri.filtering.stream import Stream


def EXAMPLE_OP(x): return x + 1


class GenericFilterTest(unittest.TestCase):

    def test_single_input_stream(self):
        # Testing the input stream is exactly the one we gave
        source_stream = Stream()
        gen_filter = GenericFilter(source_stream, EXAMPLE_OP)
        self.assertEqual(len(gen_filter.get_input_streams()), 1)
        self.assertEqual(gen_filter.get_input_streams()[0], source_stream)

    def test_single_output_stream(self):
        # Testing we get a single output stream
        source_stream = Stream()
        gen_filter = GenericFilter(source_stream, EXAMPLE_OP)
        self.assertEqual(len(gen_filter.get_output_streams()), 1)

    def test_empty_stream(self):
        # Testing a single execute call on an empty input Stream closes the output as well
        source_stream = Stream(is_closed=True)
        gen_filter = GenericFilter(source_stream, EXAMPLE_OP)
        gen_filter.execute()
        self.assertTrue(gen_filter.get_output_stream(0).is_closed())

    def test_call_after_closing(self):
        # Testing a single execute call on an empty input Stream closes the output as well
        source_stream = Stream(is_closed=True)
        gen_filter = GenericFilter(source_stream, EXAMPLE_OP)
        gen_filter.execute()
        # execute again, no error should arise
        gen_filter.execute()
        self.assertTrue(gen_filter.get_output_stream(0).is_closed())

    def test_simple_stream_applies(self):
        # Testing the method is applied to all the elements of the input stream
        expected = [EXAMPLE_OP(x) for x in range(100)]
        source_stream = Stream(list(range(100)))
        source_stream.close()
        gen_filter = GenericFilter(source_stream, EXAMPLE_OP)
        while not gen_filter.get_output_stream(0).is_closed():
            gen_filter.execute()
        self.assertEqual(gen_filter.get_output_stream(0), expected)
