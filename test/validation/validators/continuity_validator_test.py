from otri.validation.validators.continuity_validator import ContinuityValidator
from otri.validation.exceptions import ContinuityError
from otri.filtering.stream import Stream
from .. import count_errors, find_error

import unittest
from typing import Callable, Iterable


def _continuous_if_equal(first, second):
    return ContinuityError({"reason": "not equal"}) if first != second else None


class ContinuityValidatorTest(unittest.TestCase):

    def template(self, find: Callable, test_data: Iterable, expected: Iterable, continuity: Callable):
        '''
        Parameters:

            find : Callable
                Function converting an output list into some evaluable result.

            test_data : Iterable
                The test data to put in the input Streams.

            expected : Iterable
                The expected results after passing through `find`.
                Must be the same size as test_data.

            continuity : Callable
                Function defining continuity.
        '''
        inputs = Stream(test_data, is_closed=True)
        outputs = Stream()

        f = ContinuityValidator("in", "out", continuity)
        f.setup([inputs], [outputs], dict())

        while not f._are_outputs_closed():
            f.execute()

        results = list(f._get_output(0))
        prepared_outputs = find(results)

        print(results)
        self.assertListEqual(prepared_outputs, expected)

    def test_basic_stream_marks_errors(self):
        '''
        Basic streams with all atoms discontinuous.
        '''

        self.template(
            lambda data: find_error(data, ContinuityError),
            [{"number": x} for x in range(10)],
            # First and last elements get flagged once. The middle ones twice.
            [True] * 10,
            _continuous_if_equal
        )

    def test_basic_stream_count_errors(self):
        '''
        Basic streams with only last atom discontinuous.
        Check that the errors are in the right quantity.
        '''

        self.template(
            lambda data: count_errors(data, ContinuityError),
            [{"number": x} for x in range(10)],
            # First and last elements get flagged once. The middle ones twice.
            [1] + [2] * 8 + [1],
            _continuous_if_equal
        )
