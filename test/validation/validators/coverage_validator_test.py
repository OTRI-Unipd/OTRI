from otri.validation.validators.coverage_validator import CoverageValidator
from otri.validation.exceptions import CoverageError
from otri.filtering.stream import Stream
from .. import find_error, count_errors


import unittest
from parameterized import parameterized
from typing import Callable, Iterable, Mapping


class DiscrepancyValidatorTest(unittest.TestCase):

    def template(self, find: Callable, test_data: Iterable, expected: Iterable, intervals: Mapping):
        '''
        Parameters:

            find : Callable
                Function converting an output list into some evaluable result.

            test_data : Iterable
                The test data to put in the input Streams.

            expected : Iterable
                The expected results after passing through `find`.
                Must be the same size as test_data.

            limits : Mapping
                Mapping of the keys to check and their discrepancy limits.
        '''
        inputs = Stream(test_data, is_closed=True)
        outputs = Stream()

        f = CoverageValidator("in", "out", intervals)
        f.setup([inputs], [outputs], dict())

        while not f._are_outputs_closed():
            f.execute()

        result = list(f._get_output(0))
        prepared_outputs = find(result)

        self.assertListEqual(prepared_outputs, expected)

    @parameterized.expand([
        (find_error, [False] * 10),
        (count_errors, [0] * 10)
    ])
    def test_ok_stream_one_key(self, find, expected):
        '''
        Basic stream where only one key needs to be covered.
        '''
        self.template(
            lambda data: find(data, CoverageError),
            [{"number": x} for x in range(10)],
            expected,
            {"number": range(10)}
        )

    @parameterized.expand([
        (find_error, [True] + [False] * 4 + [True] + [False] * 4),
        (count_errors, [1] + [0] * 4 + [1] + [0] * 4)
    ])
    def test_not_ok_injects(self, find, expected):
        '''
        Testing that when a gap is actually found, a new atom is injected.
        '''
        self.template(
            lambda data: find(data, CoverageError),
            # First and sixth atoms should contain the error.
            [{"number": x} for x in [1, 2, 3, 4, 6, 7, 8, 9]],
            expected,
            {"number": range(10)}
        )

    @parameterized.expand([
        (find_error, [False] * 10),
        (count_errors, [0] * 10)
    ])
    def test_interval_runs_out(self, find, expected):
        '''
        Testing that after a stream runs out all atoms are ok.
        '''
        self.template(
            lambda data: find(data, CoverageError),
            [{"number": x} for x in range(10)],
            expected,
            {"number": list()}
        )

    @parameterized.expand([
        (find_error, [False] + [True] * 9 + [False] * 2),
        (count_errors, [0] + [1] * 9 + [0] * 2)
    ])
    def test_wrong_value(self, find, expected):
        '''
        When a value that won't ever be found in the interval is in the Stream, the whole interval
        is consumed.
        '''
        self.template(
            lambda data: find(data, CoverageError),
            [{"number": x} for x in [1, -1, 3]],
            expected,
            {"number": range(1, 11)}
        )
