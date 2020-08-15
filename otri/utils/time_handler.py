from datetime import datetime, time, timedelta, timezone as tz
from pytz import timezone


def str_to_datetime(string: str, tz: timezone = timezone("GMT")) -> datetime:
    year = int(string[:4])
    month = int(string[5:7])
    day = int(string[8:10])
    hours = int(string[11:13])
    minutes = int(string[14:16])
    seconds = int(string[17:19])
    micros = int(string[20:23]) * 1000
    return tz.localize(datetime(
        year=year,
        month=month,
        day=day,
        hour=hours,
        minute=minutes,
        second=seconds,
        microsecond=micros
    ))


def datetime_to_str(dt: datetime, tz: timezone = tz.utc) -> str:
    dt = dt.astimezone(tz)
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


def epoc_to_datetime(epoch: int) -> datetime:
    '''
    Converts epoch in SECONDS to datetime.
    '''
    return datetime.fromtimestamp(epoch, tz=tz.utc)


def now() -> str:
    '''
    Retruns: current datetime correctly formatted.
    '''
    return datetime_to_str(datetime.utcnow())


def local_tzinfo() -> time:
    return datetime.now().astimezone().tzinfo
