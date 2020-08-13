from typing import Final, Any


T = TypeVar('T')
K = TypeVar('K')


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

    def __init__(self, key: K, value: T, start: T = None, end: T = None, *args, **kwargs):
        '''
        If only start is passed, "higher than" is assumed as the expected result.

        If only end is passed, "lower than" is assumed as the expected result.

        If both start and end are passed, "between" is assumed as the expected result.

        Parameters:
            key : K
                The key for the value.

            value : T
                The value that triggered the error.

            start : T
                The start of the interval.

            end : T
                The end of the interval.
        '''
        if not end:
            super().__init__("key {}. Expected value > (or >=) {}. Found {}.".format(
                key, start, value
            ), *args, **kwargs)
        elif not start:
            super().__init__("key {}. Expected value < (or <=) {}. Found {}.".format(
                key, end, value
            ), *args, **kwargs)
        else:
            super().__init__("key {}. Expected {} < (or <=) value < (or <=) {}. Found {}.".format(
                key, start, end, value,
            ), *args, **kwargs)


class NullError(AtomError):
    '''
    Error for when a value is null.
    '''

    def __init__(self, key: K, *args, **kwargs):
        '''
        Parameters:
            key : K
                The key (or keys) that should not have had a null value.
        '''
        super().__init__("Expected non-null value on {} but found null.".format(
            key
        ), *args, **kwargs)


class AtomValueError(AtomError, ValueError):
    '''
    Error for an atom whose value is not accepted.
    '''

    def __init__(self, key: K, value: T, *args, **kwargs):
        '''
        Parameters:
            key : K
                The key in the atom with the error.

            value : T
                The value that triggered the error.
        '''
        super().__init__("Value for key {} : {} not valid.".format(
            key, value
        )*args, **kwargs)


class ContinuityError(AtomError):
    '''
    Error thrown when two atoms are not contiguous for some value.
    '''

    def __init__(self, key: K, first: T, second: T, *args, **kwargs):
        '''
        Parameters:
            key : K
                The key where the two non contiguous values reside.

            first : T
                The first value in the pair of atoms that triggered the error.

            second : T
                The second value in the pair.
        '''
        super().__init__("Values {} and {} for {} are not contiguous.".format(
            first, second, key
        ), *args, **kwargs)


class ContinuityWarning(AtomWarning):
    '''
    Warning thrown when two atoms might not be contiguous for some value.
    '''

    def __init__(self, key: K, first: T, second: T, *args, **kwargs):
        '''
        Parameters:
            key : K
                The key where the two non contiguous values reside.

            first : T
                The first value in the pair of atoms that triggered the warning.

            second : T
                The second value in the pair.
        '''
        super().__init__("Values {} and {} for key {} might not be contiguous, consider checking the stream.".format(
            first, second, key
        ), *args, **kwargs)
