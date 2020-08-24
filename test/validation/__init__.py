from otri.validation.exceptions import AtomError, AtomWarning, AtomException

from typing import List, Mapping, Callable, Type

AnyAtomError = Type[AtomException]
'''Generic type for an AtomException'''


def example_error_check(data: Mapping):
    '''
    Raise error on atoms with "number" key higher than 49.

    Parameters:
        data : Mapping

    Raises:
        AtomError if the "number" key value is higher than 49.
    '''
    if data["number"] > 49:
        raise AtomError("Value higher than 49.")


def example_warning_check(data: Mapping):
    '''
    Raise warning on atoms with "number" key higher than 49.

    Parameters:
        data : Mapping

    Raises:
        AtomWarning if the "number" key value is higher than 49.
    '''
    if data["number"] > 49:
        raise AtomWarning("Value higher than 49.")


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
        check(x)


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
    return [bool(key in atom.keys() and filter(error, atom[key])) for atom in data]
