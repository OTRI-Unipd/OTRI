from datetime import datetime

def str_to_datetime(string : str) -> datetime:
    return datetime.strptime(string, "%Y-%m-%d %H:%M:%S.%f")

def datetime_to_str(dt : datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]