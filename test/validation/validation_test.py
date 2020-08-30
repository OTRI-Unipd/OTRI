from otri.validation import ValidatorFilter, MonoValidator, LinearValidator, ParallelValidator
from otri.validation.exceptions import AtomError, AtomWarning, DEFAULT_KEY
from otri.filtering.stream import Stream

from . import example_error_check, example_warning_check, bulk_check, find_error

import unittest
from typing import List, Iterable, Mapping, Callable


class ValidatorFilterTest(unittest.TestCase):

    def template(self, atom: Mapping, exc: Exception, key):
        '''
        Ensures the exception gets added to key.
        '''
        f = ValidatorFilter(["in"], ["out"], 1, 1)
        f._add_label(atom, exc)
        self.assertTrue(key in atom.keys())
        self.assertTrue(str(exc) in atom[key])

    def test_add_label_no_KEY(self):
        '''
        Ensure a generic exception is added on the default key.
        '''
        self.template({"number": 1}, ValueError("Not a float."), DEFAULT_KEY)

    def test_add_label_error(self):
        '''
        Ensure an AtomError is added in the `AtomError.KEY` key.
        '''
        self.template({"number": 1}, AtomError("Not a float."), AtomError.KEY)

    def test_add_label_warning(self):
        '''
        Ensure an AtomWarning is added in the `AtomWarning.KEY` key.
        '''
        self.template({"number": 1}, AtomWarning("Not a float."), AtomWarning.KEY)


class MonoValidatorTest(unittest.TestCase):

    def template(self, check: Callable, find: Callable, test_data: Iterable, expected: Iterable):
        '''
        Parameters:
            check : Callable
                The _check method to use.

            find : Callable
                Function converting the output list into some evaluable result.

            test_data : Iterable
                The test data to put in the input Stream.

            expected : Iterable
                The expected results after passing through `find`.
        '''
        self.filter = MonoValidator(
            inputs="in",
            outputs="out",
            check=check
        )
        self.input = Stream(test_data, is_closed=True)
        self.output = Stream()
        self.state = dict()
        self.filter.setup([self.input], [self.output], self.state)

        while iter(self.input).has_next():
            self.filter.execute()

        result = list(self.output)
        self.assertListEqual(find(result), expected)

    def test_basic_error(self):
        '''
        Test for a basic check that puts errors on certain values.
        '''
        self.template(
            example_error_check,
            lambda data: find_error(data, AtomError),
            [{"number": x} for x in range(45, 55)],
            [False] * 5 + [True] * 5
        )

    def test_basic_warning(self):
        '''
        Test for a basic check that puts warnings on certain values.
        '''
        self.template(
            example_warning_check,
            lambda data: find_error(data, AtomWarning),
            [{"number": x} for x in range(45, 55)],
            [False] * 5 + [True] * 5
        )


class LinearValidatorTest(unittest.TestCase):

    def template(self, check: Callable, find: Callable, test_data: List[Iterable], expected: List[Iterable]):
        '''
        Parameters:
            check : Callable
                The _check method to use.

            find : Callable
                Function converting an output list into some evaluable result.

            test_data : List[Iterable]
                The test data to put in the input Streams. Must be a list of input datasets.

            expected : List[Iterable]
                The expected results after passing through `find`. Must be a list of expected outputs.
                Must be the same size as test_data.
        '''
        if len(test_data) != len(expected):
            raise ValueError("Lengths must be the same.")

        size = len(test_data)

        self.filter = LinearValidator(
            inputs=[str(x) for x in range(size)],
            outputs=[str(-x) for x in range(size)],
            check=check
        )
        self.inputs = [Stream(batch, is_closed=True) for batch in test_data]
        self.outputs = [Stream() for _ in range(size)]
        self.state = dict()
        self.filter.setup(self.inputs, self.outputs, self.state)

        while not self.filter._are_outputs_closed():
            self.filter.execute()

        prepared_output = [find(output) for output in self.outputs]

        # Check the output is correct, both length and values.
        self.assertListEqual(prepared_output, expected)

    def test_basic_error(self):
        '''
        Test for a basic check that puts errors on certain values.
        '''
        self.template(
            example_error_check,
            lambda data: find_error(data, AtomError),
            [[{"number": x} for x in range(100)]],
            [[False] * 50 + [True] * 50]
        )

    def test_basic_warning(self):
        '''
        Test for a basic check that puts warnings on certain values.
        '''
        self.template(
            example_warning_check,
            lambda data: find_error(data, AtomWarning),
            [[{"number": x} for x in range(100)]],
            [[False] * 50 + [True] * 50]
        )

    def test_double_uneven_input(self):
        '''
        Test for two uneven Streams.
        '''
        self.template(
            example_error_check,
            lambda data: find_error(data, AtomError),
            [[{"number": x} for x in range(25, 75)], [{"number": x} for x in range(25)]],
            [[False] * 25 + [True] * 25, [False] * 25]
        )


class ParallelValidatorTest(unittest.TestCase):

    def template(self, check: Callable, find: Callable, test_data: List[Iterable], expected: List[Iterable]):
        '''
        Parameters:
            check : Callable
                The _check method to use.

            find : Callable
                Function converting an output list into some evaluable result.

            test_data : List[Iterable]
                The test data to put in the input Streams. Must be a list of input datasets.

            expected : List[Iterable]
                The expected results after passing through `find`. Must be a list of expected outputs.
                Must be the same size as test_data.
        '''
        if len(test_data) != len(expected):
            raise ValueError("Lengths must be the same.")

        size = len(test_data)

        self.filter = ParallelValidator(
            inputs=[str(x) for x in range(size)],
            outputs=[str(-x) for x in range(size)],
            check=check
        )
        self.inputs = [Stream(batch, is_closed=True) for batch in test_data]
        self.outputs = [Stream() for _ in range(size)]
        self.state = dict()
        self.filter.setup(self.inputs, self.outputs, self.state)

        while not self.filter._are_outputs_closed():
            self.filter.execute()

        prepared_output = [find(output) for output in self.outputs]

        # Check the output is correct, both length and values.
        self.assertListEqual(prepared_output, expected)

    def test_basic_error(self):
        '''
        Test for a basic check that puts errors on certain values.
        '''
        self.template(
            lambda data, indexes: bulk_check(example_error_check, data),
            lambda data: find_error(data, AtomError),
            [[{"number": x} for x in range(100)]],
            [[False] * 50 + [True] * 50]
        )

    def test_basic_warning(self):
        '''
        Test for a basic check that puts warnings on certain values.
        '''
        self.template(
            lambda data, indexes: bulk_check(example_warning_check, data),
            lambda data: find_error(data, AtomWarning),
            [[{"number": x} for x in range(100)]],
            [[False] * 50 + [True] * 50]
        )

    def test_contagious(self):
        '''
        When the error is raises, such as in the example check, all the atoms in the batch should
        receive a label, so parallel_example_data[2][2][1], despite having values lower than 50
        should still be labeled.
        '''
        self.template(
            lambda data, indexes: bulk_check(example_error_check, data),
            lambda data: find_error(data, AtomError),
            [[{"number": x} for x in range(50, 75)], [{"number": x} for x in range(25)]],
            [[True] * 25, [True] * 25]
        )
