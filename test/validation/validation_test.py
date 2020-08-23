from otri.validation.validation import *
from otri.filtering.stream import Stream

import unittest
from typing import List, Iterable, Mapping, Callable


def example_error_check(data):
    '''
    Throw error on atoms with "number" key higher than 49.
    '''
    if data["number"] > 49:
        raise AtomError("Value higher than 49.")


def example_warning_check(data):
    '''
    Throw warning on atoms with "number" key higher than 49.
    '''
    if data["number"] > 49:
        raise AtomWarning("Value higher than 49.")


def bulk_check(check: Callable, data):
    '''
    Apply a check to a list of data.
    '''
    for x in data:
        check(x)


def find_errors(data: Iterable[Mapping]) -> List[bool]:
    '''Find atoms containing at least an error'''
    return [AtomError.KEY in x.keys() for x in data]


def find_warnings(data: Iterable[Mapping]) -> List[bool]:
    '''Find atoms containing at least a warning'''
    return [AtomWarning.KEY in x.keys() for x in data]


# Each data entry is a tuple with four entries: check method, find method, inputs, expected output.
mono_example_data = (
    # Put an error on every atom with "number" value higher than 49.
    (example_error_check,
     find_errors,
     [{"number": x} for x in range(100)],
     [False] * 50 + [True] * 50),
    # Same as above but use a warning.
    (example_warning_check,
     find_warnings,
     [{"number": x} for x in range(100)],
     [False] * 50 + [True] * 50),
)


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

        self.assertListEqual(find(self.output), expected)

    def test_basic_error(self):
        '''
        Test for a basic check that puts errors on certain values.
        '''
        self.template(*mono_example_data[0])

    def test_basic_warning(self):
        '''
        Test for a basic check that puts warnings on certain values.
        '''
        self.template(*mono_example_data[1])


# Each data entry is a tuple with four entries: check method, find method, inputs, expected output.
linear_example_data = (
    # ? Same as `mono_example_data` with a single input and output.
    # Put an error on every atom with "number" value higher than 49.
    (example_error_check,
     find_errors,
     [[{"number": x} for x in range(100)]],
     [[False] * 50 + [True] * 50]),
    # Same as above but use a warning.
    (example_warning_check,
     find_warnings,
     [[{"number": x} for x in range(100)]],
     [[False] * 50 + [True] * 50]),
    # Multiple inputs.
    (example_error_check,
     find_errors,
     [[{"number": x} for x in range(25, 75)], [{"number": x} for x in range(25)]],
     [[False] * 25 + [True] * 25, [False] * 25])
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
        self.template(*linear_example_data[0])

    def test_basic_warning(self):
        '''
        Test for a basic check that puts warnings on certain values.
        '''
        self.template(*linear_example_data[1])

    def test_double_uneven_input(self):
        '''
        Test for two uneven Streams.
        '''
        self.template(*linear_example_data[2])


# Each data entry is a tuple with four entries: check method, find method, inputs, expected output.
parallel_example_data = (
    # ? Same as `mono_example_data` with a single input and output.
    # Put an error on every atom with "number" value higher than 49.
    (lambda data: bulk_check(example_error_check, data),
     find_errors,
     [[{"number": x} for x in range(100)]],
     [[False] * 50 + [True] * 50]),
    # Same as above but use a warning.
    (lambda data: bulk_check(example_warning_check, data),
     find_warnings,
     [[{"number": x} for x in range(100)]],
     [[False] * 50 + [True] * 50]),
    # Multiple inputs.
    (lambda data: bulk_check(example_error_check, data),
     find_errors,
     [[{"number": x} for x in range(50, 75)], [{"number": x} for x in range(25)]],
     [[True] * 25, [True] * 25])
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
        self.template(*parallel_example_data[0])

    def test_basic_warning(self):
        '''
        Test for a basic check that puts warnings on certain values.
        '''
        self.template(*parallel_example_data[1])

    def test_contagious(self):
        '''
        When the error is raises, such as in the example check, all the atoms in the batch should
        receive a label, so parallel_example_data[2][2][1], despite having values lower than 50
        should still be labeled.
        '''
        self.template(*parallel_example_data[2])
