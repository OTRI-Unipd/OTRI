from typing import Final, Any


DEFAULT_KEY: Final = "UNKNOWN"


class AtomError(Exception):
    '''
    Raiseable class used to represent an error found in an atom's data.
    Used to indicate some kind of error that makes the atom impossible or dangerous to use.
    '''
    KEY: Final = "ERROR"


class AtomWarning(Exception):
    '''
    Raiseable class used to represent a warning regarding an atom's data.
    Used to indicate a chance that the atom may or may not have an error, but not enough elements
    are available to determine whether that is the case.
    '''
    KEY: Final = "WARNING"


class RangeError(AtomError):
    '''
    Error for an out of range value.

    Since paramters are cast to string, they should be human readable.
    '''

    def __init__(self, key: Any, value: Any, start: Any = None, end: Any = None, *args, **kwargs):
        '''
        If only start is passed, "higher than" is assumed as the expected result.

        If only end is passed, "lower than" is assumed as the expected result.

        If both start and end are passed, "between" is assumed as the expected result.

        Parameters:
            key : Any
                The key for the value.

            value : Any
                The value that triggered the error.

            start : Any
                The start of the interval.

            end : Any
                The end of the interval.
        '''
        if not end:
            super().__init__("RangeError: key {}. Expected value > {}. Found {}.".format(
                ket, start, value
            ), *args, **kwargs)
        elif not start:
            super().__init__("RangeError: key {}. Expected value < {}. Found {}.".format(
                ket, end, value
            ), *args, **kwargs)
        else:
            super().__init__("RangeError: key {}. Expected {} < value < {}. Found {}.".format(
                key, start, end, value,
            ), *args, **kwargs)


class NullError(AtomError):
    '''
    Error for when a value is null.
    '''

    def __init__(self, key: Any, *args, **kwargs):
        '''
        Parameters:
            key : Any
                The key (or keys) that should not have had a null value.
        '''
        super().__init__("NullError: Expected non-null value on {} but found null.".format(
            key
        ), *args, **kwargs)
