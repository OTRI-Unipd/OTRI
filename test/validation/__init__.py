from otri.validation.exceptions import AtomError, AtomWarning, AtomException

from typing import List, Mapping, Callable, Type, Optional
import re

AnyAtomError = Type[AtomException]
'''Generic type for an AtomException'''


def example_error_check(data: Mapping) -> Optional[AtomError]:
    '''
    Raise error on atoms with "number" key higher than 49.

    Parameters:
        data : Mapping

    Returns:
        An `AtomError` if `data["number"]` > 49.
    '''
    if data["number"] > 49:
        return AtomError("Value higher than 49.")


def example_warning_check(data: Mapping) -> Optional[AtomWarning]:
    '''
    Raise warning on atoms with "number" key higher than 49.

    Parameters:
        data : Mapping

    Returns:
        An `AtomWarning` if `data["number"]` > 49.
    '''
    if data["number"] > 49:
        return AtomWarning("Value higher than 49.")


def bulk_check(check: Callable, data: List[Mapping]):
    '''
    Apply a check to a list of data.

    Parameters:
        check : Callable
            A check to pass through all in a list of atoms.

        data : List
            The list of data to check.
    '''
    for x in data:
        result = check(x)
        if result is not None:
            return result


def is_error_string(string: str, error: AnyAtomError) -> bool:
    '''
    Parameters:
        string : str
            The string.

        error : AnyAtomError
            Any AtomException subclass.

    Returns:
        True if the string is in the form: "error_class_name(...)".
    '''
    error_string = "{}\\(.*\\)".format(error.__name__)
    return bool(re.match(error_string, string))


def find_error(data: List[Mapping], error: AnyAtomError) -> List[bool]:
    '''
    Use this to find a certain error in a List.

    Parameters:
        data : List[Mapping]
            The data to check.

        error : AnyAtomError
            The error to find.

    Returns:
        List[bool] : An item is True only if the corrisponding item in `data` has error.KEY in its
        keys and an instance of such error.
    '''
    key = error.KEY
    result = list()
    for atom in data:
        if key not in atom.keys():
            result.append(False)
        else:
            errors = filter(lambda x: is_error_string(x, error), atom[key])
            result.append(bool(len(list(errors)) > 0))
    return result


def count_errors(data: List[Mapping], error: AnyAtomError) -> List[int]:
    '''
    Use this to count how many errors of a certain type a list of atoms has.

    Parameters:
        data : List[Mapping]
            The data to check.

        error : AnyAtomError
            The error of which to count occurrences.

    Returns:
        List[int] : Each item is how many errors of type `error` the respective atom contained.
    '''
    key = error.KEY
    result = list()
    for atom in data:
        if key not in atom.keys():
            result.append(0)
        else:
            errors = filter(lambda x: is_error_string(x, error), atom[key])
            result.append(len(list(errors)))
    return result
