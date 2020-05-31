import unittest
from otri.filtering.filters.validator_filter import ValidatorFilter
from otri.filtering.stream import Stream

EXAMPLE_DATA = [1, 2, 3, 4, 5]
EXPECTED = [3, 4]
CHECKS = [
    lambda x: True if x > 1 else False,
    lambda y: True if y > 2 else False,
    lambda z: True if z < 5 else False
]


class ValidatorFilterTest(unittest.TestCase):

    def test_single_input_stream(self):
        # Testing the input stream is exactly the one we gave
        source_stream = Stream()
        ex_filter = ValidatorFilter(source_stream, CHECKS)
        self.assertEqual(len(ex_filter.get_input_streams()), 1)

    def test_single_output_stream(self):
        # Testing we get a single output
        source_stream = Stream()
        ex_filter = ValidatorFilter(source_stream, CHECKS)
        self.assertEqual(len(ex_filter.get_input_streams()), 1)

    def test_empty_stream(self):
        # Testing a single execute call on an empty input Stream closes the output as well
        source_stream = Stream(is_closed=True)
        ex_filter = ValidatorFilter(source_stream, CHECKS)
        ex_filter.execute()
        self.assertTrue(ex_filter.get_output_stream(0).is_closed())

    def test_call_after_closing(self):
        # Testing a single execute call on an empty input Stream closes the output as well
        source_stream = Stream(is_closed=True)
        ex_filter = ValidatorFilter(source_stream, CHECKS)
        ex_filter.execute()
        # execute again, no error should arise
        ex_filter.execute()
        self.assertTrue(ex_filter.get_output_stream(0).is_closed())

    def test_all_checks_behave(self):
        source_stream = Stream(EXAMPLE_DATA, is_closed=True)
        ex_filter = ValidatorFilter(source_stream, CHECKS)
        while not ex_filter.is_finished():
            ex_filter.execute()
        # Weird error when casting to list
        self.assertEqual(ex_filter.get_output_stream(0), EXPECTED)
