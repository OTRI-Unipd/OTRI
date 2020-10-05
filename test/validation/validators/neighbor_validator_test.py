from otri.validation.validators.neighbor_validator import NeighborValidator
from otri.validation.exceptions import NeighborWarning
from otri.filtering.stream import Stream
from .. import find_error

import unittest
from typing import Callable, Iterable, Mapping


class NeighborValidatorTest(unittest.TestCase):

    def template(self, find: Callable, test_data: Iterable, expected: Iterable, t_range: int, limits: Mapping):
        '''
        Parameters:

            find : Callable
                Function converting an output list into some evaluable result.

            test_data : Iterable
                The test data to put in the input Streams. Must be a list of input datasets.

            expected : Iterable
                The expected results after passing through `find`. Must be a list of expected outputs.
                Must be the same size as test_data.

            t_range : int
                The time range for the validation.

            limits : Mapping
                Mapping of the keys to check and their discrepancy limits.
        '''
        inputs = [Stream(batch, is_closed=True) for batch in test_data]
        outputs = [Stream() for _ in test_data]

        f = NeighborValidator([str(i) for i in range(len(inputs))],
                              [str(-i) for i in range(len(outputs))],
                              t_range,
                              limits)
        f.setup(inputs, outputs, dict())

        while not f._are_outputs_closed():
            f.execute()

        results = [list(output) for output in f._get_outputs()]
        prepared_outputs = [find(output) for output in results]

        for i in range(len(prepared_outputs)):
            self.assertListEqual(prepared_outputs[i], expected[i])

    def test_basic_streams(self):
        '''
        Basic streams with only last atom discrepant.
        '''
        self.template(
            lambda data: find_error(data, NeighborWarning),
            [[{"v": x} for x in range(10)], [{"v": x} for x in [0, 1, 2, 3, 4, 100, 6, 7, 8, 9]]],
            # The only item with no neighbor is the middle element of the second Stream.
            [[False] * 10, [False] * 5 + [True] + [False] * 4],
            2,
            {"v": 0.5}
        )

    def test_uneven_streams(self):
        '''
        Test with uneven stream.
        '''
        # 1 2 3 4 5 6 7 8 9 10
        # 0 1 2 3 4
        self.template(
            lambda data: find_error(data, NeighborWarning),
            [[{"v": x} for x in range(1, 11)], [{"v": x} for x in range(5)]],
            [[False] * 4 + [True] * 4 + [False] * 2, [False] * 5],
            2,
            {"v": 0.1}
        )

    def test_more_than_two(self):
        '''
        Test with multiple streams.
        '''
        self.template(
            lambda data: find_error(data, NeighborWarning),
            # Inputs
            [[{"v": x} for x in range(1, 12)],
             [{"v": x} for x in [1, 2, 3, 4, 6]],
             [{"v": x} for x in list(range(1, 9)) + [15, 11, 12]]],
            # Expected
            [[False] * 8 + [True] + [False] * 2,
             [False] * 5,
             [False] * 8 + [True] + [False] * 2],
            2,
            {"v": 0.1}
        )

    def test_multiple_keys(self):
        '''
        Test on multiple keys at the same time.
        '''
        self.template(
            lambda data: find_error(data, NeighborWarning),
            # Inputs
            [[{"one": 10, "two": -10}, {"one": 20, "two": -20}, {"one": 30, "two": -30}],
             [{"one": 10, "two": -10}, {"one": 23, "two": -20}, {"one": 30, "two": -30}],
             [{"one": 10, "two": -12}, {"one": 20, "two": -20}, {"one": 30, "two": -40}]],
            # Expected
            [[False, False, False],
             [False, True, False],
             [False, False, False]],
            1,
            {"one": 0.1, "two": 0.1}
        )
