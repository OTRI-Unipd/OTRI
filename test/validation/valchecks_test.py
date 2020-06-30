import unittest
from datetime import date
from typing import List, Callable
from otri.filtering.stream import Stream
from otri.validation.validation import ValidatorFilter
from otri.validation.valchecks import *


ex_start_date = date(2020, 4, 10)
ex_end_date = date(2020, 4, 20)
ex_valid_date = date(2020, 4, 15)
ex_invalid_date = date(2020, 4, 5)


class CheckDateBetweenTest(unittest.TestCase):

    def test_returns_callable(self):
        self.assertTrue(isinstance(
            make_check_date_between(ex_start_date, ex_end_date), Callable)
        )

    def test_valid_date_true(self):
        method = make_check_date_between(ex_start_date, ex_end_date)
        self.assertEqual((ValidatorFilter.OK, None), method(ex_valid_date))

    def test_invalid_date_false(self):
        method = make_check_date_between(ex_start_date, ex_end_date)
        self.assertEqual(ValidatorFilter.ERROR, method(ex_invalid_date)[0])

    def test_equal_false(self):
        method = make_check_date_between(ex_start_date, ex_end_date)
        self.assertEqual(ValidatorFilter.ERROR, method(ex_start_date)[0])

    def test_equal_true_if_inclusive(self):
        method = make_check_date_between(
            ex_start_date, ex_end_date, inclusive=True)
        self.assertEqual((ValidatorFilter.OK, None), method(ex_start_date))

    # Same as above with parameter dates order reversed

    def test_returns_callable_reversed(self):
        self.assertTrue(isinstance(
            make_check_date_between(ex_end_date, ex_start_date), Callable)
        )

    def test_valid_date_true_reversed(self):
        method = make_check_date_between(ex_end_date, ex_start_date)
        self.assertEqual((ValidatorFilter.OK, None), method(ex_valid_date))

    def test_invalid_date_false_reversed(self):
        method = make_check_date_between(ex_end_date, ex_start_date)
        self.assertEqual(ValidatorFilter.ERROR, method(ex_invalid_date)[0])

    def test_equal_false_reversed(self):
        method = make_check_date_between(ex_end_date, ex_start_date)
        self.assertEqual(ValidatorFilter.ERROR, method(ex_start_date)[0])

    def test_equal_true_if_inclusive_reversed(self):
        method = make_check_date_between(
            ex_end_date, ex_start_date, inclusive=True)
        self.assertEqual((ValidatorFilter.OK, None), method(ex_start_date))
