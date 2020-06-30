from ..validation.validation import ValidatorFilter
from datetime import datetime
from typing import Callable, Mapping, Tuple


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
