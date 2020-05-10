from otri.validation.validation import BaseValidator
from typing import List, Callable
import otri.validation.validation as validation
import unittest
from datetime import date


class BaseValidatorTest(unittest.TestCase):

    def test_get_checks_returns_list(self):
        # Checking the return type for BaseValidator.get_checks()
        self.assertTrue(isinstance(BaseValidator().get_checks(), List))

    def test_starts_empty(self):
        # Checking the list of checks starts empty
        self.assertFalse(BaseValidator().get_checks())

    def test_can_add_one(self):
        # Testing the list of check increases in size
        def foo(x): return True
        valid = BaseValidator()
        valid.add_checks(foo)
        self.assertEqual(len(valid.get_checks()), 1)

    def test_one_is_added(self):
        # Testing the actual passed method is added to the list
        def foo(x): return True
        valid = BaseValidator()
        valid.add_checks(foo)
        self.assertCountEqual(valid.get_checks(), [foo])

    def test_can_add_more(self):
        # Same as previous, with multiple callables
        foos = [lambda x: True, lambda y: False]
        valid = BaseValidator()
        valid.add_checks(*foos)
        self.assertEqual(len(valid.get_checks()), len(foos))

    def test_more_are_added(self):
        # Same as previous, with multiple callables
        foos = [lambda x: True, lambda y: False]
        valid = BaseValidator()
        valid.add_checks(*foos)
        self.assertCountEqual(valid.get_checks(), foos)

    def test_can_remove_one(self):
        # Testing removal of a single check
        def foo(x): return True
        valid = BaseValidator()
        valid.add_checks(foo)
        valid.remove_checks(foo)
        self.assertFalse(valid.get_checks())

    def test_one_is_removed(self):
        # Testing that exactly one got removed if there are more
        foos = [lambda x: True, lambda y: False]
        valid = BaseValidator()
        valid.add_checks(*foos)
        valid.remove_checks(foos[0])
        self.assertCountEqual(valid.get_checks(), [foos[1]])

    def test_can_remove_more(self):
        # Testing that more checks can get removed
        foos = [lambda x: True, lambda y: False]
        valid = BaseValidator()
        valid.add_checks(*foos)
        valid.remove_checks(*foos)
        self.assertFalse(valid.get_checks())

    def test_more_are_removed(self):
        # Testing that exactly the removed multiple checks get removed
        foos = [lambda x: True, lambda y: False, lambda z: 3.14]
        valid = BaseValidator()
        valid.add_checks(*foos)
        valid.remove_checks(foos[0], foos[1])
        self.assertCountEqual(valid.get_checks(), [foos[2]])

    def test_all_checks_behave(self):
        foos = [lambda x: True if x else False,
                lambda y: False if y else True, lambda z: 3.14 if z else "bruh"]
        valid = BaseValidator()
        valid.add_checks(*foos)
        self.assertEqual(
            valid.validate(list()),
            {foos[0]: False, foos[1]: True, foos[2]: "bruh"}
        )
        self.assertEqual(
            valid.validate([1, 2, 3]),
            {foos[0]: True, foos[1]: False, foos[2]: 3.14}
        )


ex_start_date = date(2020, 4, 10)
ex_end_date = date(2020, 4, 20)
ex_valid_date = date(2020, 4, 15)
ex_invalid_date = date(2020, 4, 5)


class CheckDateBetweenTest(unittest.TestCase):
    
    def test_returns_callable(self):
        self.assertTrue(isinstance(
            validation.make_check_date_between(ex_start_date, ex_end_date), Callable)
        )

    def test_valid_date_true(self):
        method = validation.make_check_date_between(ex_start_date, ex_end_date)
        self.assertTrue(method(ex_valid_date))

    def test_invalid_date_false(self):
        method = validation.make_check_date_between(ex_start_date, ex_end_date)
        self.assertFalse(method(ex_invalid_date))

    def test_equal_false(self):
        method = validation.make_check_date_between(ex_start_date, ex_end_date)
        self.assertFalse(method(ex_start_date))

    def test_equal_true_if_inclusive(self):
        method = validation.make_check_date_between(ex_start_date, ex_end_date, inclusive=True)
        self.assertTrue(method(ex_start_date))

    # Same as above with parameter dates order reversed

    def test_returns_callable_reversed(self):
        self.assertTrue(isinstance(
            validation.make_check_date_between(ex_end_date, ex_start_date), Callable)
        )

    def test_valid_date_true_reversed(self):
        method = validation.make_check_date_between(ex_end_date, ex_start_date)
        self.assertTrue(method(ex_valid_date))

    def test_invalid_date_false_reversed(self):
        method = validation.make_check_date_between(ex_end_date, ex_start_date)
        self.assertFalse(method(ex_invalid_date))

    def test_equal_false_reversed(self):
        method = validation.make_check_date_between(ex_end_date, ex_start_date)
        self.assertFalse(method(ex_start_date))

    def test_equal_true_if_inclusive_reversed(self):
        method = validation.make_check_date_between(ex_end_date, ex_start_date, inclusive=True)
        self.assertTrue(method(ex_start_date))