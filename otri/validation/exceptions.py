from typing import Final, Any, TypeVar

T = TypeVar('T')
K = TypeVar('K')


DEFAULT_KEY: Final = "UNKNOWN"


class AtomException(Exception):
    '''
    Base class for Exceptions on Atoms.
    '''

    KEY: Final = DEFAULT_KEY

    def __init__(self, msg: str, **args):
        '''
        Parameters:
            msg : str
                Base message for the Exception.

            args :
                Optional arguments, expected to be key : value pairs.
        '''
        if args:
            line = "key = {} : value = {}\n"
            for k, v in args.items():
                msg = ''.join([msg, line.format(k, v)])
        super().__init__(msg)


class AtomError(AtomException):
    '''
    Raiseable class used to represent an error found in an atom's data.
    Used to indicate some kind of error that makes the atom impossible or dangerous to use.
    '''
    KEY: Final = "ERROR"


class AtomWarning(AtomException):
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

    def __init__(self, start: T, end: T, **args):
        '''
        If only start is passed, "higher than" is assumed as the expected result.

        If only end is passed, "lower than" is assumed as the expected result.

        If both start and end are passed, "between" is assumed as the expected result.

        Parameters:
            start : T
                The start of the interval.

            end : T
                The end of the interval.

            args :
                The key value pairs for the error, see `AtomException` for details.
        '''
        if not end:
            super().__init__("Values did not satisfy: X > (or >=) {}.\n".format(start), **args)
        elif not start:
            super().__init__("Values did not satisfy: X < (or <=) {}.\n".format(end), **args)
        else:
            super().__init__("Values did not satisfy: {} < (or <=) value < (or <=) {}.\n".format(
                start, end
            ), **args)


class NullError(AtomError):
    '''
    Error for when a value is None or otherwise null.
    '''

    def __init__(self, **args):
        '''
        See `AtomException` for details.
        '''
        super().__init__("Expected non-null values.\n", **args)


class AtomValueError(AtomError, ValueError):
    '''
    Error for an atom whose value is not accepted.
    '''

    def __init__(self, **args):
        '''
        See `AtomException` for details.
        '''
        super().__init__("Values made no sense or were not allowed.\n", **args)


class ContinuityError(AtomError):
    '''
    Error thrown when two atoms are not contiguous for some value.
    '''

    def __init__(self, **args):
        '''
        See `AtomException` for details.
        '''
        super().__init__("Discontinuous values found.\n", **args)


class ContinuityWarning(AtomWarning):
    '''
    Warning thrown when two atoms might not be contiguous for some value.
    '''

    def __init__(self, **args):
        '''
        See `AtomException` for details.
        '''
        super().__init__("Values might be discontinuous, consider checking the Stream.\n", **args)


class ClusterWarning(AtomWarning):
    '''
    Warning thrown when a cluster of values is found on contiguous atoms.
    '''

    def __init__(self, **args):
        '''
        See `AtomException` for details.
        '''
        super().__init__("Cluster found on atoms. Consider checking the Stream.\n", **args)
