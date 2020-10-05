from otri.validation.validators.discrepancy_validator import DiscrepancyValidator
from otri.validation.exceptions import DiscrepancyError
from otri.filtering.stream import Stream
from .. import find_error

import unittest
from typing import Callable, Iterable, Mapping


class DiscrepancyValidatorTest(unittest.TestCase):

    def template(self, find: Callable, test_data: Iterable, expected: Iterable, limits: Mapping):
        '''
        Parameters:

            find : Callable
                Function converting an output list into some evaluable result.

            test_data : Iterable
                The test data to put in the input Streams. Must be a list of input datasets.

            expected : Iterable
                The expected results after passing through `find`. Must be a list of expected outputs.
                Must be the same size as test_data.

            limits : Mapping
                Mapping of the keys to check and their discrepancy limits.
        '''
        inputs = [Stream(batch, is_closed=True) for batch in test_data]
        outputs = [Stream() for _ in test_data]

        f = DiscrepancyValidator([str(i) for i in range(len(inputs))],
                                 [str(-i) for i in range(len(outputs))],
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
            lambda data: find_error(data, DiscrepancyError),
            [[{"number": x} for x in [1, 2, 3, 4, 5]], [{"number": x} for x in [1, 2, 3, 4, 6]]],
            [[False] * 4 + [True], [False] * 4 + [True]],
            {"number": 0.1}
        )

    def test_uneven_streams(self):
        '''
        Test with uneven stream.
        '''
        self.template(
            lambda data: find_error(data, DiscrepancyError),
            [[{"number": x} for x in range(1, 11)], [{"number": x} for x in [1, 2, 3, 4, 6]]],
            [[False] * 4 + [True] + [False] * 5, [False] * 4 + [True]],
            {"number": 0.1}
        )

    def test_more_than_two(self):
        '''
        Test with multiple streams.
        '''
        self.template(
            lambda data: find_error(data, DiscrepancyError),
            # Inputs
            [[{"number": x} for x in range(1, 11)],
             [{"number": x} for x in [1, 2, 3, 4, 6]],
             [{"number": x} for x in list(range(1, 9)) + [11, 12]]],
            # Expected
            [[False] * 4 + [True] + [False] * 3 + [True] * 2,
             [False] * 4 + [True],
             [False] * 4 + [True] + [False] * 3 + [True] * 2],
            {"number": 0.1}
        )

    def test_multiple_keys(self):
        '''
        Test on multiple keys at the same time.
        '''
        self.template(
            lambda data: find_error(data, DiscrepancyError),
            # Inputs
            [[{"one": 1, "two": -1}, {"one": 2, "two": -2}, {"one": 3, "two": -3}],
             [{"one": 1, "two": -1}, {"one": 3, "two": -2}, {"one": 3, "two": -3}],
             [{"one": 1, "two": -1}, {"one": 2, "two": -2}, {"one": 3, "two": -4}]],
            # Expected
            [[False, True, True],
             [False, True, True],
             [False, True, True]],
            {"one": 0.1, "two": 0.1}
        )
