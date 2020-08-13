from .validation import ValidatorFilter
from .exceptions import RangeError
from typing import Callable, Mapping, Tuple, TypeVar

T = TypeVar('T')
K = TypeVar('K')


def make_check_range(key: K, value1: T, value2: T, inclusive: bool = False) -> Callable[[Mapping[K, T]]]:
    '''
    Return a method that looks into an atom, on a certain key, and finds out whether its value is
    in a certain range, and raises an error if it's not.

    Parameters:
        key : K
            The key for which to check the value.

        value1 : T
            The first value

        value2 : T
            The second value

        inclusive : bool = False
            Whether to include the two values in the accepted interval.

    Returns:
        Callable[[Mapping[K, T]]] : Checks whether the given value is in the estabilished range. The
        minimum between the two given values is used as the first. Raises a `RangeError` if the
        value is not in range.
    '''
    start = min([value1, value2])
    end = max([value1, value2])

    if inclusive:
        def check_range(atom: Mapping[K, T]):
            value = atom[key]
            if not (start <= value <= end):
                raise RangeError(key, value, start, end)
    else:
        def check_range(atom: Mapping[K, T]):
            value = atom[key]
            if not (start < value < end):
                raise RangeError(key, value, start, end)

    return check_range


def check_positive(atom: Mapping[K, T], keys: List[K], zero_positive: bool = True):
    '''
    Check whether values for keys in the atom are positive. If one or more are negative it raises a
    `RangeError`.

    Parameters:
        atom : Mapping[K, T]
            The atom to check.

        keys : List[K]
            The list of keys for which to check the values.

        zero_positive : bool
            If `True`, 0 counts as positive, else as negative. Default is `True`.
    '''
    faulty_keys = list()
    faulty_values = list()
    for k in keys:
        if zero_positive:
            v = atom[k]
            if v < 0:
                faulty_keys.append(k)
                faulty_values.append(v)
        else:
            v = atom[k]
            if atom[k] <= 0:
                faulty_keys.append(k)
                faulty_values.append(v)
    if faulty_keys:
        raise RangeError(faulty_keys, faulty_values, 0)


def check_non_null(atom: Mapping[K, T], keys: List[K]):
    '''
    Check whether values for keys in the atom are non null. If one or more are, it raises a
    `NullError`.

    Parameters:
        atom : Mapping[K, T]
            The atom to check.

        keys : List[K]
            The list of keys for which to check the values.
    '''
    faulty_keys = list()
    for k in keys:
        if atom[k] == None:
            faulty_keys.append(k)
    if faulty_keys:
        raise NullError(faulty_keys)
