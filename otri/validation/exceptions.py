from typing import TypeVar, Mapping

T = TypeVar('T')
K = TypeVar('K')


DEFAULT_KEY = "UNKNOWN"


class AtomException:
    '''
    Base class for Exceptions on Atoms.
    '''

    KEY = DEFAULT_KEY

    def __init__(self, msg: str, reason: Mapping = dict(), *args, **kwargs):
        '''
        Parameters:
            msg : str
                Base message for the Exception.

            reason : Mapping
                Expected to be key : value pairs.
        '''
        if args:
            line = "key = {} : value = {}\n"
            for k, v in args.items():
                msg = ''.join([msg, line.format(k, v)])
        self.msg = msg

    def __str__(self):
        '''
        Returns:
            String representation for the object: "classname(message)".
        '''
        return self.__class__.__name__ + "({})".format(self.msg)

    def __repr__(self):
        '''
        Same as `__str__`.
        '''
        return str(self)


class AtomError(AtomException):
    '''
    Raiseable class used to represent an error found in an atom's data.
    Used to indicate some kind of error that makes the atom impossible or dangerous to use.
    '''
    KEY = "ERROR"


class AtomWarning(AtomException):
    '''
    Raiseable class used to represent a warning regarding an atom's data.
    Used to indicate a chance that the atom may or may not have an error, but not enough elements
    are available to determine whether that is the case.
    '''
    KEY = "WARNING"


class RangeError(AtomError):
    '''
    Error for an out of range value.

    Since paramters are cast to string, they should be human readable.
    '''

    def __init__(self, start: T, end: T, reason: Mapping = dict(), *args, **kwargs):
        '''
        If only start is passed, "higher than" is assumed as the expected result.

        If only end is passed, "lower than" is assumed as the expected result.

        If both start and end are passed, "between" is assumed as the expected result.

        Parameters:
            start : T
                The start of the interval.

            end : T
                The end of the interval.

            reason : Mapping
                Expected to be key : value pairs.
        '''
        if not end:
            super().__init__("Values did not satisfy: X > (or >=) {}.\n".format(start),
                             reason, *args, **kwargs)
        elif not start:
            super().__init__("Values did not satisfy: X < (or <=) {}.\n".format(end),
                             reason, *args, **kwargs)
        else:
            super().__init__("Values did not satisfy: {} < (or <=) value < (or <=) {}.\n".format(
                start, end
            ), reason, *args, **kwargs)


class NullError(AtomError):
    '''
    Error for when a value is None or otherwise null.
    '''

    def __init__(self, reason: Mapping = dict(), *args, **kwargs):
        '''
        See `AtomException` for details.
        '''
        super().__init__("Expected non-null values.\n", reason, *args, **kwargs)


class AtomValueError(AtomError, ValueError):
    '''
    Error for an atom whose value is not accepted.
    '''

    def __init__(self, reason: Mapping = dict(), *args, **kwargs):
        '''
        See `AtomException` for details.
        '''
        super().__init__("Values made no sense or were not allowed.\n", reason, *args, **kwargs)


class ContinuityError(AtomError):
    '''
    Error thrown when two atoms are not contiguous for some value.
    '''

    def __init__(self, reason: Mapping = dict(), *args, **kwargs):
        '''
        See `AtomException` for details.
        '''
        super().__init__("Discontinuous values found.\n", reason, *args, **kwargs)


class ContinuityWarning(AtomWarning):
    '''
    Warning thrown when two atoms might not be contiguous for some value.
    '''

    def __init__(self, reason: Mapping = dict(), *args, **kwargs):
        '''
        See `AtomException` for details.
        '''
        super().__init__("Values might be discontinuous, consider checking the Stream.\n",
                         reason, *args, **kwargs)


class ClusterWarning(AtomWarning):
    '''
    Warning thrown when a cluster of values is found on contiguous atoms.
    '''

    def __init__(self, reason: Mapping = dict(), *args, **kwargs):
        '''
        See `AtomException` for details.
        '''
        super().__init__("Cluster found on atoms. Consider checking the Stream.\n",
                         reason, *args, **kwargs)


class DiscrepancyError(AtomError):
    '''
    Error raised on Streams that present a certain discrepancy level.
    '''

    def __init__(self, level: float, reason: Mapping = dict(), *args, **kwargs):
        '''
        Parameters:
            level : float
                The discrepancy level that was exceeded.

        See `AtomException` for details.
        '''
        super().__init__("Discrepancy higher than {} found on Streams (stream index: value).\n"
                         .format(level), reason, *args, **kwargs)


class CoverageError(AtomError):
    '''
    Error raised on Streams where coverage of a certain value list is expected but nothing is found.
    '''

    def __init__(self, reason: Mapping = dict(), *args, **kwargs):
        '''
        See `AtomException` for details.
        '''
        super().__init__("No coverage found for (key : value):\n", reason, *args, **kwargs)


class NeighborWarning(AtomWarning):
    '''
    Warning indicating that an atom has no neighbors among streams parallel to it.
    '''
    def __init__(self, reason: Mapping = dict(), *args, **kwargs):
        '''
        See `AtomException` for details.
        '''
        super().__init__("No neighbors found for atom.\n", reason, *args, **kwargs)
