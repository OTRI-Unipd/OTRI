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

    def setUp(self):
        self.input = Stream(EXAMPLE_DATA, closed=True)
        self.output = [Stream() for _ in range(4)]
        self.output_w_none = [Stream() for _ in range(5)]
        self.f = SplitFilter(
            inputs="A",
            outputs=["B", "C", "D", "E"],
            key=KEY,
            ranges=VALUES,
            none_keys_output=["None"]
        )

    def test_outputs_ignore_none(self):
        f = SplitFilter(
            inputs="A",
            outputs=["B", "C", "D", "E"],
            key=KEY,
            ranges=VALUES,
            none_keys_output=None
        )
        self.assertEqual(len(f.output_names), len(VALUES)+1)

    def test_outputs_include_none(self):
        self.assertEqual(len(self.f.output_names), len(VALUES)+2)

    def test_simple_stream_ignore_none_left(self):
        # Testing the result for a simple Stream, while ignoring atoms that do not have the key.
        f = SplitFilter(
            inputs="A",
            outputs=["B", "C", "D", "E"],
            key=KEY,
            ranges=VALUES,
            none_keys_output=None,
            side='left'
        )
        f.setup([self.input], self.output, None)
        while not self.output[0].is_closed():
            f.execute()
        self.assertEqual(self.output, SPLIT_L)

    def test_simple_stream_ignore_none_right(self):
        # Testing the result for a simple Stream, while ignoring atoms that do not have the key.
        f = SplitFilter(
            inputs="A",
            outputs=["B", "C", "D", "E"],
            key=KEY,
            ranges=VALUES,
            none_keys_output=None,
            side='right'
        )
        f.setup([self.input], self.output, None)
        while not self.output[0].is_closed():
            f.execute()
        self.assertEqual(self.output, SPLIT_R)

    def test_simple_stream_split_none_left(self):
        # Testing the result for a simple Stream, while ignoring atoms that do not have the key.
        f = SplitFilter(
            inputs="A",
            outputs=["B", "C", "D", "E"],
            key=KEY,
            ranges=VALUES,
            none_keys_output="F",
            side='left'
        )
        f.setup([self.input], self.output_w_none, None)
        while not self.output_w_none[0].is_closed():
            f.execute()
        self.assertEqual(self.output_w_none, SPLIT_L_NONE)

    def test_simple_stream_split_none_right(self):
        # Testing the result for a simple Stream, while splitting atoms that do not have the key.
        f = SplitFilter(
            inputs="A",
            outputs=["B", "C", "D", "E"],
            key=KEY,
            ranges=VALUES,
            none_keys_output="F",
            side='right'
        )
        f.setup([self.input], self.output_w_none, None)
        while not self.output_w_none[0].is_closed():
            f.execute()
        self.assertEqual(self.output_w_none, SPLIT_R_NONE)


class SwitchFilterTest(unittest.TestCase):

    def setUp(self):
        self.input = Stream(EXAMPLE_DATA, closed=True)
        self.output = [Stream() for _ in range(4)]
        self.output_w_none = [Stream() for _ in range(5)]
        self.f = SwitchFilter(
            inputs="A",
            cases_outputs=["B", "C", "D"],
            default_output="Default",
            key=KEY,
            cases=VALUES
        )

    def test_outputs_ignore_none(self):
        self.assertEqual(len(self.f.output_names), len(VALUES)+1)

    def test_outputs_include_none(self):
        f = SwitchFilter(
            inputs="A",
            cases_outputs=["B", "C", "D"],
            default_output="Default",
            key=KEY,
            cases=VALUES,
            none_keys_output="None"
        )
        self.assertEqual(len(f.output_names), len(VALUES)+2)

    def test_empty_stream(self):
        # Testing a single execute call on an empty input Stream closes the output as well
        self.f.setup([Stream(closed=True)],self.output,None)
        self.f.execute()
        self.assertTrue(self.output[0].is_closed())

    def test_call_after_closing(self):
        # Testing a single execute call on an empty input Stream closes the output as well
        self.f.setup([Stream(closed=True)],self.output,None)
        self.f.execute()
        # execute again, no error should arise
        self.f.execute()
        self.assertTrue(self.output[0].is_closed())

    def test_get_case_output(self):
        # Testing the requested case output is retrieved.
        f = SwitchFilter(
            inputs="A",
            cases_outputs=["B", "C", "D"],
            default_output="Default",
            key=KEY,
            cases=VALUES
        )
        f.setup([Stream([{KEY : 1}], closed=True)],self.output, None)
        f.execute()
        self.assertEqual(self.output[0], [{KEY: 1}])

    def test_simple_stream_ignore_none(self):
        # Testing the result for a simple Stream, while ignoring atoms that do not have the key.
        self.f.setup([self.input],self.output,None)
        while not self.output[0].is_closed():
            self.f.execute()
        self.assertCountEqual(self.output, SWITCH)
        # Ensure default is last
        self.assertEqual(self.output[-1], SWITCH[-1])

    def test_simple_stream_include_none(self):
        # Testing the result for a simple Stream, while preserving atoms that do not have the key.
        f = SwitchFilter(
            inputs="A",
            cases_outputs=["B", "C", "D"],
            default_output="Default",
            key=KEY,
            cases=VALUES,
            none_keys_output="None"
        )
        f.setup([self.input],self.output_w_none,None)
        while not self.output_w_none[0].is_closed():
            f.execute()
        self.assertCountEqual(self.output_w_none, SWITCH_NONE)
        # Ensure default is second from the end
        self.assertEqual(self.output_w_none[-2], SWITCH_NONE[-2])
        # Ensure None is last
        self.assertEqual(self.output_w_none[-1], SWITCH_NONE[-1])
