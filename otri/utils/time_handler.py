from datetime import datetime, date, time, timedelta
from pytz import timezone, utc
from tzlocal import get_localzone


def str_to_datetime(string: str, tz: timezone = timezone("GMT")) -> datetime:
    year = int(string[:4])
    month = int(string[5:7])
    day = int(string[8:10])
    hours = int(string[11:13] or 0)
    minutes = int(string[14:16] or 0)
    seconds = int(string[17:19] or 0)
    micros = int(string[20:23] or 0) * 1000
    return tz.localize(datetime(
        year=year,
        month=month,
        day=day,
        hour=hours,
        minute=minutes,
        second=seconds,
        microsecond=micros
    ))


def datetime_to_str(dt: datetime, tz: timezone = timezone("GMT")) -> str:
    if dt.tzinfo is not None:
        dt = dt.astimezone(utc)
    return "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}.{:03d}".format(
        dt.year,
        dt.month,
        dt.day,
        dt.hour,
        dt.minute,
        dt.second,
        int(dt.microsecond/1000)
    )


def datetime_to_epoch(dt: datetime) -> int:
    '''
    Returns: datetime to epoch in seconds.
    '''
    return int(dt.timestamp())


def sum_time(t: time, td: timedelta) -> time:
    tmp_dt = datetime.combine(datetime(1, 1, 1), t) + td
    return tmp_dt.timetz()


def epoch_to_datetime(epoch: int, tz: timezone = timezone("GMT")) -> datetime:
    '''
    Converts epoch in SECONDS to datetime.
    '''
    return datetime.fromtimestamp(epoch, tz=tz)


def now() -> str:
    '''
    Retruns: current datetime correctly formatted.
    '''
    return datetime_to_str(datetime.utcnow())


def local_tzinfo() -> timezone:
    return get_localzone()


def sub_times(t1: time, t2: time) -> int:
    '''
    Returns:
        Total seconds between the two times.\n
    '''
    tmp_dt1 = datetime.combine(datetime(1, 1, 1), t1)
    tmp_dt2 = datetime.combine(datetime(1, 1, 1), t2)
    return (tmp_dt1 - tmp_dt2).total_seconds()
