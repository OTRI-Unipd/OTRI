import otri.validation.validation as validation
import unittest
from datetime import date
from copy import deepcopy
from typing import List, Callable
from otri.filtering.stream import Stream
from otri.validation.validation import ValidatorFilter

test_data = [
    {"Hello": "Hello"},
    {"Yeet": "Coca cola can"},
    {"Python 2": "Is alright"},
    {"I'm OK": "Hopefully"},
    {"Same": "Here"}
]


def EXAMPLE_CHECK(atom, neigh):
    '''
    Having "Yeet" key is ERROR.
    Having "Python 2" key is WARNING.
    Neighbor is always only previous atom.
    '''
    if "Yeet" in atom.keys():
        return ValidatorFilter.ERROR, "Oh no, this is an error."
    if "Python 2" in atom.keys():
        return ValidatorFilter.WARNING, "Oh well, this is a warning."
    return ValidatorFilter.OK, "Everything is fine."

# Considering data and check above:
# test_data[1] has an ERROR, so does its neighbor test_data[0]
# test_data[2] has a WARNING, so does its neighbor test_data[1]
# test_data[3] and test_data[4] should be OK.


ex_start_date = date(2020, 4, 10)
ex_end_date = date(2020, 4, 20)
ex_valid_date = date(2020, 4, 15)
ex_invalid_date = date(2020, 4, 5)


class ValidatorFilterTest(unittest.TestCase):
    # Setup method, ran before each test.
    def setUp(self):
        self.__setup()
        self.__run()
        return super().setUp()

    # Setup test data.
    def __setup(self):
        self.state = dict()
        self.output = Stream()
        self.backup = deepcopy(test_data)
        self.input = Stream(test_data, is_closed=True)
        self.filter = ValidatorFilter("in", "out", [EXAMPLE_CHECK])
        self.filter.setup(
            inputs=[self.input],
            outputs=[self.output],
            state=self.state
        )

    # Pass the whole stream through the filter.
    def __run(self):
        while self.input:
            self.filter.execute()

    # Testing an error is found on a single atom.
    def test_finds_error(self):
        # The first atom is fine in this test data.
        self.assertTrue(ValidatorFilter.ERR_KEY in self.output[1].keys())

    # Testing a warning is found on a single atom.
    def test_finds_warning(self):
        # The first and second atoms have no direct warning.
        self.assertTrue(ValidatorFilter.WARN_KEY in self.output[2].keys())

    # Testing atoms that are ok are not modified.
    def test_ok_are_unmodified(self):
        self.assertTrue(
            self.backup[3] == self.output[3] and
            self.backup[4] == self.output[4]
        )

    # TODO Not implemented
    # Testing an error propagates to the neighbors.
    def test_error_neighbors(self):
        # First element has no error itself but is the neighbor of one that does.
        self.assertTrue(ValidatorFilter.ERR_KEY in self.output[0].keys())

    # TODO Not implemented
    def test_warn_neighbors(self):
        # Second element has no warning itself but is the neighbor of one that does.
        self.assertTrue(ValidatorFilter.WARN_KEY in self.output[1].keys())


class CheckDateBetweenTest(unittest.TestCase):

    def test_returns_callable(self):
        self.assertTrue(isinstance(
            validation.make_check_date_between(ex_start_date, ex_end_date), Callable)
        )

    def test_valid_date_true(self):
        method = validation.make_check_date_between(ex_start_date, ex_end_date)
        self.assertEqual((ValidatorFilter.OK, None), method(ex_valid_date))

    def test_invalid_date_false(self):
        method = validation.make_check_date_between(ex_start_date, ex_end_date)
        self.assertEqual(ValidatorFilter.ERROR, method(ex_invalid_date)[0])

    def test_equal_false(self):
        method = validation.make_check_date_between(ex_start_date, ex_end_date)
        self.assertEqual(ValidatorFilter.ERROR, method(ex_start_date)[0])

    def test_equal_true_if_inclusive(self):
        method = validation.make_check_date_between(
            ex_start_date, ex_end_date, inclusive=True)
        self.assertEqual((ValidatorFilter.OK, None), method(ex_start_date))

    # Same as above with parameter dates order reversed

    def test_returns_callable_reversed(self):
        self.assertTrue(isinstance(
            validation.make_check_date_between(ex_end_date, ex_start_date), Callable)
        )

    def test_valid_date_true_reversed(self):
        method = validation.make_check_date_between(ex_end_date, ex_start_date)
        self.assertEqual((ValidatorFilter.OK, None), method(ex_valid_date))

    def test_invalid_date_false_reversed(self):
        method = validation.make_check_date_between(ex_end_date, ex_start_date)
        self.assertEqual(ValidatorFilter.ERROR, method(ex_invalid_date)[0])

    def test_equal_false_reversed(self):
        method = validation.make_check_date_between(ex_end_date, ex_start_date)
        self.assertEqual(ValidatorFilter.ERROR, method(ex_start_date)[0])

    def test_equal_true_if_inclusive_reversed(self):
        method = validation.make_check_date_between(
            ex_end_date, ex_start_date, inclusive=True)
        self.assertEqual((ValidatorFilter.OK, None), method(ex_start_date))
