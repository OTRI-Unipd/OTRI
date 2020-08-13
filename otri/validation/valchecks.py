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
        With the earliest date as start date. Returns `True` or `False`.
    '''
    start = min([date1, date2])
    end = max([date1, date2])

    if inclusive:
        def check_date(date: datetime):
            return start <= date <= end
    else:
        def check_date(date: datetime):
            return start < date < end

    return check_date
