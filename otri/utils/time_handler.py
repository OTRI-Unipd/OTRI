from datetime import datetime, time, timedelta, timezone


def str_to_datetime(string: str) -> datetime:
    year = int(string[:4])
    month = int(string[5:7])
    day = int(string[8:10])
    hours = int(string[11:13])
    minutes = int(string[14:16])
    seconds = int(string[17:19])
    micros = int(string[20:23]) * 1000
    return datetime(
        year=year,
        month=month,
        day=day,
        hour=hours,
        minute=minutes,
        second=seconds,
        microsecond=micros,
        tzinfo=timezone.utc
    )


def datetime_to_str(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


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
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


def now() -> str:
    '''
    Retruns: current datetime correctly formatted.
    '''
    return datetime_to_str(datetime.utcnow())
