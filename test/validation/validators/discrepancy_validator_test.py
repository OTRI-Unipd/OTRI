from otri.validation.validators.discrepancy_validator import DiscrepancyValidator
from otri.validation.exceptions import DiscrepancyError
from otri.filtering.stream import Stream

import unittest


class DiscrepancyValidatorTest(unittest.TestCase):

    def test_basic_stream(self):
        '''
        Basic Streams, only last element in both Streams is to be flagged.
        '''
        inputs = [
            Stream([{"number": x} for x in [1, 2, 3, 4, 5]], is_closed=True),
            Stream([{"number": x} for x in [1, 2, 3, 4, 6]], is_closed=True)
        ]
        outputs = [Stream(), Stream()]
        expected = [False] * 4 + [True]

        f = DiscrepancyValidator(["1", "2"], ["-1", "-2"], {"number": 0.1})
        f.setup(inputs, outputs, dict())

        while not f._are_outputs_closed():
            f.execute()

        KEY = DiscrepancyError.KEY

        def right_error(item):
            return isinstance(item, DiscrepancyError)

        results = [list(f._get_output(0)), list(f._get_output(1))]
        prepared_outputs = [
            [bool(KEY in atom.keys() and filter(right_error, atom[KEY])) for atom in results[0]],
            [bool(KEY in atom.keys() and filter(right_error, atom[KEY])) for atom in results[1]]
        ]

        print(results)
        self.assertListEqual(prepared_outputs[0], expected)
        self.assertListEqual(prepared_outputs[0], expected)
