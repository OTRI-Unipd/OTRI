from typing import Sequence, Callable, Mapping, List, Any, Set, Tuple, Final
from ..filtering.filter import Filter
from ..filtering.stream import Stream
from datetime import datetime


class ValidatorFilter(Filter):

    '''
    This Filter is used to apply a check to a list of atoms.
    This is an abstract class and should be further extended implementing the `_check(atom)` method.
    '''

    OK: Final = -1
    UNCLEAR: Final = 0
    ERROR: Final = 1
    WARNING: Final = 2
    ERR_KEY: Final = "error"
    WARN_KEY: Final = "warning"

    # Mapping methods to avoid if-else chain.
    __ACTIONS = {
        ValidatorFilter.OK: ValidatorFilter._on_ok,
        ValidatorFilter.UNCLEAR: ValidatorFilter._on_unclear,
        ValidatorFilter.ERROR: ValidatorFilter._on_error,
        ValidatorFilter.WARNING: ValidatorFilter._on_warning
    }

    def _on_data(self, data, index):
        '''
        Called when input data is found.

        Parameters:
            data : Any
                Popped data from an input.
            index : int
                The index of the input the data has been popped from.
        Returns:
            The result of the check on the data (to allow extension with more cases).
        '''
        result = self._check(data)
        ValidatorFilter.__ACTIONS[result](self, data)
        return result

    # ? MANDATORY OVERRIDE ---

    def _check(self, data) -> Tuple[int, str]:
        '''
        Check a single atom. Must be implemented.
        Should return one of `ValidatorFilter.OK`, `ValidatorFilter.UNCLEAR`, `ValidatorFilter.WARNING`
        or `ValidatorFilter.ERROR` and an informative optional message.

        Parameters:
            data
                The data to check.
        '''
        raise NotImplementedError("ValidatorFilter is an abstract class, please extend it.")

    # ? OPTIONAL OVERRIDE ---

    def _on_ok(self, data):
        '''
        Called if data resulted ok.

        Parameters:
            data
                The checked data.
        '''
        pass

    def _on_unclear(self, data):
        '''
        Called if data resulted ok.

        Parameters:
            data
                The checked data.
        '''
        pass

    def _on_error(self, data):
        '''
        Called if data resulted ok.

        Parameters:
            data
                The checked data.
        '''
        pass

    def _on_warning(self, data):
        '''
        Called if data resulted ok.

        Parameters:
            data
                The checked data.
        '''
        pass

    # ? STATIC METHODS ---

    @staticmethod
    def __add_label(atom, key, value):
        '''
        Add an error or warning label to an atom.
        Add an empty `list` in `atom` for `key` if `key` is not present, then append `value`.
        '''
        if key not in atom.keys():
            atom[key] = list()
        atom[key].append(value)


def make_check_date_between(date1: datetime, date2: datetime, inclusive: bool = False) -> Callable[[datetime, Mapping], Tuple[int, str]]:
    '''
    Parameters:
        date1 : datetime
            The first date
        date2 : datetime
            The second date
        inclusive : bool = False
            Whether to include the two dates in the accepted interval

    Returns:
        Callable[datetime] : Checks whether the given datetime is in-between the two given dates.
        Will choose the smallest date as start date
    '''
    start = min([date1, date2])
    end = max([date1, date2])
    inclusive = inclusive

    def check_date_between(d: datetime, state: Mapping = None) -> Tuple[bool, str]:
        '''
        Parameters:
            d : datetime
                A date to check
            state : Mapping
                State for this check, unused.
        Returns:
            `ValidatorFilter.OK` and None if parameter d is after start and before end.
            `ValidatorFilter.ERROR` and a string error if it isn't. If inclusive is False also
            does not consider `start` and `end` as "between".
            Does NOT check timezones.
        '''
        if inclusive:
            if start <= d and d <= end:
                return ValidatorFilter.OK, None
            else:
                return ValidatorFilter.ERROR,
                "Date not between {} and {} (inclusive).".format(start, end)
        else:
            if start < d and d < end:
                return ValidatorFilter.OK, None
            else:
                return ValidatorFilter.ERROR,
                "Date not between {} and {} (non-inclusive).".format(start, end)

    return check_date_between
