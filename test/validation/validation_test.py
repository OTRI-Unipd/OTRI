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
    return ATOM_OK


def example_warning_check(data):
    '''
    Throw warning on atoms with "number" key higher than 49.
    '''
    if data["number"] > 49:
        raise AtomWarning("Value higher than 49.")
    return ATOM_OK


def find_errors(data: Iterable[Mapping]) -> List[bool]:
    '''Find atoms containing at least an error'''
    return [AtomError.KEY in x.keys() for x in data]


def find_warnings(data: Iterable[Mapping]) -> List[bool]:
    '''Find atoms containing at least a warning'''
    return [AtomWarning.KEY in x.keys() for x in data]


example_data = [{"number": x} for x in range(100)]
expected_result = [False] * 50 + [True] * 50


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

    def template(self, check: Callable, test_data: Iterable, expected: Iterable, find: Callable):
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

        self.assertListEqual(find(self.output), list(expected))

    def test_basic_error(self):
        '''
        Test for a basic check that puts errors on certain values.
        '''
        self.template(example_error_check, example_data, expected_result, find_errors)

    def test_basic_warning(self):
        '''
        Test for a basic check that puts warnings on certain values.
        '''
        self.template(example_warning_check, example_data, expected_result, find_warnings)
