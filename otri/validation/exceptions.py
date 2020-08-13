from typing import Final


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
