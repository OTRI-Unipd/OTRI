import unittest
from otri.filtering.filters.generic_filter import GenericFilter
from otri.filtering.stream import Stream


def EXAMPLE_OP(x): return x + 1


class GenericFilterTest(unittest.TestCase):

    def setUp(self):
        self.input_stream = Stream()
        self.output_stream = Stream()
        self.gen_filter = GenericFilter(
            input="A", output="B", operation=EXAMPLE_OP)

    def test_empty_stream(self):
        # Testing a single execute call on an empty input Stream closes the output as well
        self.input_stream.close()
        self.gen_filter.setup([self.input_stream], [self.output_stream], None)
        self.gen_filter.execute()
        self.assertTrue(self.output_stream.is_closed())

    def test_call_after_closing(self):
        self.gen_filter.setup([self.input_stream], [self.output_stream], None)
        self.input_stream.close()
        # Testing a single execute call on an empty input Stream closes the output as well
        self.gen_filter.execute()
        # execute again, no error should arise
        self.gen_filter.execute()
        self.assertTrue(self.output_stream.is_closed())

    def test_simple_stream_applies(self):
        # Testing the method is applied to all the elements of the input stream
        expected = [EXAMPLE_OP(x) for x in range(100)]
        source_stream = Stream(list(range(100)), is_closed=True)
        self.gen_filter.setup([source_stream], [self.output_stream], None)
        while not self.output_stream.is_closed():
            self.gen_filter.execute()
        self.assertEqual(self.output_stream, expected)
