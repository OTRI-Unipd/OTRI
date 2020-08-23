import unittest
from datetime import date
from typing import List, Callable, Mapping
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

    @parameterized.expand([
        (make_check_set({"1": [1, 2, 3], "2":[4, 5, 6]}), [{"1": 1, "2": 4}], None),
        (make_check_set({"1": [1, 2, 3], "2":[4, 5, 6]}), [{"1": 1, "2": 3}], AtomValueError),
        (make_check_set({"1": [1, 2, 3], "2":[4, 5, 6]}), [{"1": 4, "2": 4}], AtomValueError),
        (make_check_set({"1": [1, 2, 3], "2":[4, 5, 6]}), [{"1": 4, "2": 3}], AtomValueError),
        (make_check_set({"1": [1, 2, 3]}), [{"1": 3, "2": 3}], None),
        (make_check_set({"2": [4, 5, 6]}), [{"1": 4, "2": 4}], None)
    ])
    def test_check_set(self, method, params, expected):
        return self._base_test(method, params, expected)

    @parameterized.expand([
        (make_check_range(["datetime"], ex_start_date, ex_end_date), [ex_valid_date], None),
        (make_check_range(["datetime"], ex_start_date, ex_end_date), [ex_invalid_date], RangeError),
        (make_check_range(["datetime"], ex_start_date, ex_end_date),
         [{"datetime": ex_start_date}], RangeError),
        (make_check_range(["datetime"], ex_start_date, ex_end_date, True),
         [{"datetime": ex_start_date}], None),
        # Reversed dates to ensure min and max are found.
        (make_check_range(["datetime"], ex_end_date, ex_start_date), [ex_valid_date], None),
        (make_check_range(["datetime"], ex_end_date, ex_start_date), [ex_invalid_date], RangeError),
        (make_check_range(["datetime"], ex_end_date, ex_start_date),
         [{"datetime": ex_start_date}], RangeError),
        (make_check_range(["datetime"], ex_end_date, ex_start_date, True),
         [{"datetime": ex_start_date}], None)
    ])
    def test_check_range(self, method, params, expected):
        return self._base_test(method, params, expected)


class FactoryPersistencyTest(unittest.TestCase):

    def test_persistency(self):
        '''
        Method testing factory fabricated methods persist their parameters.
        '''
        method1 = make_check_set({"a": [1, 2, 3]})
        method2 = make_check_set({"b": [1, 2, 3]})

        # If the parameters in the first method did not persist it should fail finding "b".
        self.assertIsNone(method1({"a": 1}))
        self.assertIsNone(method2({"b": 1}))
