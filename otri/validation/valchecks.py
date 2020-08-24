from .exceptions import RangeError, NullError, AtomValueError
from typing import Callable, Mapping, TypeVar, List, Any, Iterable

T = TypeVar('T')
K = TypeVar('K')


def make_check_range(keys: K, value1: T, value2: T, inclusive: bool = False) -> Callable[[Mapping[K, T]], None]:
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
        Callable[[Mapping[K, T]], None] : Checks whether the given value is in the estabilished
        range. The minimum between the two given values is used as the first. Raises a `RangeError`
        if the value is not in range.
    '''
    start = min([value1, value2])
    end = max([value1, value2])

    if inclusive:
        def check_range(atom: Mapping[K, T]):
            faulty_keys = list()
            for k in keys:
                if not (start <= atom[k] <= end):
                    faulty_keys.append(k)
            if faulty_keys:
                raise RangeError(start, end, {k: atom[k] for k in faulty_keys})

    else:
        def check_range(atom: Mapping[K, T]):
            faulty_keys = list()
            for k in keys:
                if not (start < atom[k] < end):
                    faulty_keys.append(k)
            if faulty_keys:
                raise RangeError(start, end, {k: atom[k] for k in faulty_keys})

    return check_range


def make_check_set(values: Mapping[Any, Iterable]) -> Callable[[Mapping[K, T]], None]:
    '''
    Return a method that looks into an atom and ensures every given key has a value in the given
    set.

    Parameters:
        values : Mapping
            A mapping containing the keys to check and the allowed values. The allowed values should
            be a `Set` but any `Iterable` goes.

    Returns:
        Callable[[Mapping[K, T]], None] : Checks whether the keys have values in the accepted sets.
        Raises an `AtomValueError` if they don't.
    '''
    def check_set(atom: Mapping[K, T]):
        faulty_keys = list()
        for k, allowed in values.items():
            if atom[k] not in allowed:
                faulty_keys.append(k)
        if faulty_keys:
            raise AtomValueError({k: atom[k] for k in faulty_keys})
    return check_set


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
        raise RangeError(0, None, dict(zip(faulty_keys, faulty_values)))


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
        if atom[k] is None:
            faulty_keys.append(k)
    if faulty_keys:
        raise NullError({k: atom[k] for k in faulty_keys})
