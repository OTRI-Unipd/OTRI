from otri.filtering.filters.split_filter import SplitFilter, SwitchFilter
from otri.filtering.stream import Stream
import unittest

VALUES = (1, 2, 3)
KEY = "key"
NONE_ATOM = {"Hello": "There"}
EXAMPLE_DATA = [{KEY: x} for x in range(5)] + [NONE_ATOM]

SPLIT_L = [
    [{KEY: 0}, {KEY: 1}],
    [{KEY: 2}],
    [{KEY: 3}],
    [{KEY: 4}]
]
SPLIT_R = [
    [{KEY: 0}],
    [{KEY: 1}],
    [{KEY: 2}],
    [{KEY: 3}, {KEY: 4}]
]
SPLIT_L_NONE = SPLIT_L + [[NONE_ATOM]]
SPLIT_R_NONE = SPLIT_R + [[NONE_ATOM]]

SWITCH = [
    # Ordering is random for exact values
    [{KEY: 1}], [{KEY: 2}], [{KEY: 3}],
    # Default
    [{KEY: 0}, {KEY: 4}]
]
SWITCH_NONE = SWITCH + [[NONE_ATOM]]


class SplitFilterTest(unittest.TestCase):

    def test_one_input(self):
        ex_filter = SplitFilter(Stream(), KEY, VALUES)
        self.assertEqual(len(ex_filter.get_input_streams()), 1)

    def test_outputs_ignore_none(self):
        ex_filter = SplitFilter(Stream(), KEY, VALUES)
        self.assertEqual(len(ex_filter.get_output_streams()), len(VALUES)+1)

    def test_outputs_include_none(self):
        ex_filter = SplitFilter(Stream(), KEY, VALUES, ignore_none=False)
        self.assertEqual(len(ex_filter.get_output_streams()), len(VALUES)+2)

    def test_empty_stream(self):
        # Testing a single execute call on an empty input Stream closes the output as well
        ex_filter = SplitFilter(Stream(is_closed=True), KEY, VALUES)
        ex_filter.execute()
        self.assertTrue(ex_filter.get_output_stream(0).is_closed())

    def test_call_after_closing(self):
        # Testing a single execute call on an empty input Stream closes the output as well
        ex_filter = SplitFilter(Stream(is_closed=True), KEY, VALUES)
        ex_filter.execute()
        # execute again, no error should arise
        ex_filter.execute()
        self.assertTrue(ex_filter.get_output_stream(0).is_closed())

    def test_simple_stream_ignore_none_left(self):
        # Testing the result for a simple Stream, while ignoring atoms that do not have the key.
        ex_filter = SplitFilter(
            Stream(EXAMPLE_DATA, is_closed=True),
            KEY, VALUES, side='left'
        )
        while not ex_filter.is_finished():
            ex_filter.execute()
        self.assertEqual(ex_filter.get_output_streams(), SPLIT_L)

    def test_simple_stream_ignore_none_right(self):
        # Testing the result for a simple Stream, while ignoring atoms that do not have the key.
        ex_filter = SplitFilter(
            Stream(EXAMPLE_DATA, is_closed=True),
            KEY, VALUES, side='right'
        )
        while not ex_filter.is_finished():
            ex_filter.execute()
        self.assertEqual(ex_filter.get_output_streams(), SPLIT_R)

    def test_simple_stream_split_none_left(self):
        # Testing the result for a simple Stream, while ignoring atoms that do not have the key.
        ex_filter = SplitFilter(
            Stream(EXAMPLE_DATA, is_closed=True),
            KEY, VALUES, ignore_none=False, side='left'
        )
        while not ex_filter.is_finished():
            ex_filter.execute()
        self.assertEqual(ex_filter.get_output_streams(), SPLIT_L_NONE)

    def test_simple_stream_split_none_right(self):
        # Testing the result for a simple Stream, while splitting atoms that do not have the key.
        ex_filter = SplitFilter(
            Stream(EXAMPLE_DATA, is_closed=True),
            KEY, VALUES, ignore_none=False, side='right'
        )
        while not ex_filter.is_finished():
            ex_filter.execute()
        self.assertEqual(ex_filter.get_output_streams(), SPLIT_R_NONE)


class SplitFilterTest(unittest.TestCase):

    def test_one_input(self):
        ex_filter = SwitchFilter(Stream(), KEY, VALUES)
        self.assertEqual(len(ex_filter.get_input_streams()), 1)

    def test_outputs_ignore_none(self):
        ex_filter = SwitchFilter(Stream(), KEY, VALUES)
        self.assertEqual(len(ex_filter.get_output_streams()), len(VALUES)+1)

    def test_outputs_include_none(self):
        ex_filter = SwitchFilter(Stream(), KEY, VALUES, ignore_none=False)
        self.assertEqual(len(ex_filter.get_output_streams()), len(VALUES)+2)

    def test_empty_stream(self):
        # Testing a single execute call on an empty input Stream closes the output as well
        ex_filter = SwitchFilter(Stream(is_closed=True), KEY, VALUES)
        ex_filter.execute()
        self.assertTrue(ex_filter.get_output_stream(0).is_closed())

    def test_call_after_closing(self):
        # Testing a single execute call on an empty input Stream closes the output as well
        ex_filter = SwitchFilter(Stream(is_closed=True), KEY, VALUES)
        ex_filter.execute()
        # execute again, no error should arise
        ex_filter.execute()
        self.assertTrue(ex_filter.get_output_stream(0).is_closed())

    def test_simple_stream_ignore_none(self):
        # Testing the result for a simple Stream, while ignoring atoms that do not have the key.
        ex_filter = SwitchFilter(
            Stream(EXAMPLE_DATA, is_closed=True),
            KEY, VALUES
        )
        while not ex_filter.is_finished():
            ex_filter.execute()
        outputs = ex_filter.get_output_streams()
        self.assertCountEqual(outputs, SWITCH)
        # Ensure default is last
        self.assertEqual(outputs[-1], SWITCH[-1])

    def test_simple_stream_switch_none(self):
        # Testing the result for a simple Stream, while preserving atoms that do not have the key.
        ex_filter = SwitchFilter(
            Stream(EXAMPLE_DATA, is_closed=True),
            KEY, VALUES, ignore_none=False
        )
        while not ex_filter.is_finished():
            ex_filter.execute()
        outputs = ex_filter.get_output_streams()
        self.assertCountEqual(outputs, SWITCH_NONE)
        # Ensure default is second from the end
        self.assertEqual(outputs[-2], SWITCH_NONE[-2])
        # Ensure None is last
        self.assertEqual(outputs[-1], SWITCH_NONE[-1])
