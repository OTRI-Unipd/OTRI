from datetime import datetime, time, timedelta, timezone
from typing import Union


def str_to_datetime(string: str) -> datetime:
    return datetime.strptime(string, "%Y-%m-%d %H:%M:%S.%f")


def datetime_to_str(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def datetime_to_epoch(dt: datetime) -> int:
    return int((dt - datetime(1970, 1, 1)).total_seconds())


def datetime_to_time(dt: datetime) -> time:
    '''
    Removes year month and year from a datetime.
    '''
    return time(hour=dt.hour, minute=dt.minute, second=dt.second, microsecond=dt.microsecond)


def sum_time(t: time, td: timedelta) -> time:
    tmp_dt = datetime.combine(datetime(1, 1, 1), t) + td
    return tmp_dt.time()


def epoc_to_datetime(epoch: int):
    '''
    Converts epoch in SECONDS to datetime
    '''
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


def now() -> str:
    return datetime_to_str(datetime.utcnow())
