import unittest
from datetime import date
from typing import List, Callable
from inspect import isclass
from parameterized import parameterized
from otri.filtering.stream import Stream
from otri.validation.validation import ValidatorFilter
from otri.validation.valchecks import *
from otri.validation.exceptions import *


ex_start_date = date(2020, 4, 10)
ex_end_date = date(2020, 4, 20)
ex_valid_date = {"datetime": date(2020, 4, 15)}
ex_invalid_date = {"datetime": date(2020, 4, 5)}


class ChecksTest(unittest.TestCase):
    '''
    Parameterized class parameters:\n
        "method" : the method to test.\n
        "params" : the parameters for the method (list, will be unpacked).\n
        "expected" : the expected result from the method. If list, assertListEqual will be used,
        if Exception assertRaises will be used. All the other cases default to assertEqual.
    '''

    def _base_test(self, method, params, expected):
        if isinstance(expected, List):
            self.assertListEqual(method(*params), expected)
        elif isclass(expected) and issubclass(expected, Exception):
            self.assertRaises(expected, method, *params)
        else:
            self.assertEqual(method(*params), expected)

    @parameterized.expand([
        (check_non_null, [{"1": 1, "2": 2, "3": 3}, ["1", "2", "3"]], None),
        (check_non_null, [{"1": 1, "2": 2, "3": None}, ["1", "2", "3"]], NullError),
        (check_non_null, [{"1": 1, "2": None, "3": 3}, ["1", "2", "3"]], NullError),
        (check_non_null, [{"1": None, "2": 2, "3": 3}, ["1", "2", "3"]], NullError),
        (check_non_null, [{"1": None, "2": None, "3": None}, ["1", "2", "3"]], NullError),
        (check_non_null, [{"1": 1, "2": 2, "3": None}, ["1", "2"]], None),
        (check_non_null, [{"1": 1, "2": None, "3": 3}, ["1", "3"]], None),
        (check_non_null, [{"1": None, "2": 2, "3": 3}, ["2", "3"]], None)
    ])
    def test_non_null(self, method, params, expected):
        return self._base_test(method, params, expected)

    @parameterized.expand([
        (check_positive, [{"1": 1, "2": 2, "3": 3}, ["1", "2", "3"], False], None),
        (check_positive, [{"1": 1, "2": 2, "3": 0}, ["1", "2", "3"], False], RangeError),
        (check_positive, [{"1": 1, "2": 0, "3": 3}, ["1", "2", "3"], False], RangeError),
        (check_positive, [{"1": 0, "2": 2, "3": 3}, ["1", "2", "3"], False], RangeError),
        (check_positive, [{"1": 0, "2": 0, "3": 0}, ["1", "2", "3"], False], RangeError),
        (check_positive, [{"1": 1, "2": 2, "3": 3}, ["1", "2", "3"], True], None),
        (check_positive, [{"1": 1, "2": 2, "3": 0}, ["1", "2", "3"], True], None),
        (check_positive, [{"1": 1, "2": 0, "3": 3}, ["1", "2", "3"], True], None),
        (check_positive, [{"1": 0, "2": 2, "3": 3}, ["1", "2", "3"], True], None),
        (check_positive, [{"1": 0, "2": 0, "3": 0}, ["1", "2", "3"], True], None),
        (check_positive, [{"1": 1, "2": 2, "3": 0}, ["1", "2"], False], None),
        (check_positive, [{"1": 1, "2": 0, "3": 3}, ["1", "3"], False], None),
        (check_positive, [{"1": 0, "2": 2, "3": 3}, ["2", "3"], False], None)
    ])
    def test_check_positive(self, method, params, expected):
        return self._base_test(method, params, expected)


class CheckDateBetweenTest(unittest.TestCase):

    def test_returns_callable(self):
        self.assertTrue(isinstance(
            make_check_range(["datetime"], ex_start_date, ex_end_date), Callable)
        )

    def test_valid_date_true(self):
        method = make_check_range(["datetime"], ex_start_date, ex_end_date)
        self.assertIsNone(method(ex_valid_date))

    def test_invalid_date_false(self):
        method = make_check_range(["datetime"], ex_start_date, ex_end_date)
        self.assertRaises(RangeError, method, ex_invalid_date)

    def test_equal_false(self):
        method = make_check_range(["datetime"], ex_start_date, ex_end_date)
        self.assertRaises(RangeError, method, {"datetime": ex_start_date})

    def test_equal_true_if_inclusive(self):
        method = make_check_range(
            ["datetime"], ex_start_date, ex_end_date, inclusive=True
        )
        self.assertIsNone(method({"datetime": ex_start_date}))

    # Same as above with parameter dates order reversed

    def test_returns_callable_reversed(self):
        self.assertTrue(isinstance(
            make_check_range(["datetime"], ex_end_date, ex_start_date), Callable)
        )

    def test_valid_date_true_reversed(self):
        method = make_check_range(["datetime"], ex_end_date, ex_start_date)
        self.assertIsNone(method(ex_valid_date))

    def test_invalid_date_false_reversed(self):
        method = make_check_range(["datetime"], ex_end_date, ex_start_date)
        self.assertRaises(RangeError, method, ex_invalid_date)

    def test_equal_false_reversed(self):
        method = make_check_range(["datetime"], ex_end_date, ex_start_date)
        self.assertRaises(RangeError, method, {"datetime": ex_start_date})

    def test_equal_true_if_inclusive_reversed(self):
        method = make_check_range(
            ["datetime"], ex_end_date, ex_start_date, inclusive=True
        )
        self.assertIsNone(method({"datetime": ex_start_date}))
