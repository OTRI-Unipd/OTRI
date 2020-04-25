from alpha_vantage.timeseries import TimeSeries
from download.timeseries_downloader import TimeseriesDownloader, Union
from datetime import date, datetime
from pytz import timezone
import json

GMT = timezone("GMT")
TIME_ZONE_KEY = "6. Time Zone"


class AVDownloader(TimeseriesDownloader):
    '''
    TODO: class specifications
    '''

    def __init__(self, api_key: str):
        '''
        Init method.
        Parameters:
            key : str
                the Alpha Vantage API key to use
        '''
        self.ts = TimeSeries(api_key)

    def download_between_dates(self, ticker: str, start_date: date, end_date: date, interval: str = "1m", debug: bool = False) -> Union[dict,bool]:
        '''
        Downloads quote data for a single ticker given the start date and end date.

        Parameters:
            ticker : str
                The simbol to download data of.
            start_datetime : datetime
                Must be before end_datetime.
            end_datetime : datetime
                Must be after and different from start_datetime.
            interval : str
                Could be "1min", "5min", "15min", "30min", "60min" or "1m", "5m", "15m", "30m", "60m"
        Returns:
            False if there has been an error,
            a dict containing "metadata" and "atoms" otherwise.

            metadata is a dict containing at least:
                - ticker
                - interval
                - provider
                - other data that the atomizer could want to apply to every atom

            atoms is a list of dicts containing:
                - datetime (format Y-m-d H:m:s.ms)
                - other financial values
        '''
        interval = AVDownloader.__standardize_interval(interval)
        try:
            data, meta = self.ts.get_intraday(
                ticker, interval=interval, outputsize='full')
            new_data = dict()
            new_data['metadata'] = {
                "ticker": ticker, "interval": interval, "provider": "alpha vantage"}
            new_data['atoms'] = list()
            tz = meta[TIME_ZONE_KEY]
            for k, v in data.items():
                # Datetime key and atom value
                v['datetime'] = AVDownloader.__convert_to_gmt(date_time=datetime.strptime(
                    k, "%Y-%m-%d %H:%M:%S"), zonename=tz).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                new_data['atoms'].append(v)

            return new_data
        except ValueError as exception:
            if debug: print("AV ValueError: ",exception)
            return False

    @staticmethod
    def __convert_to_gmt(*, date_time: datetime, zonename: str) -> datetime:
        '''
        Method to convert a datetime in a certain timezone to a GMT datetime
        Parameters:
            date_time : datetime
                The datetime to convert
            zonename : str
                The time zone's name
        Returns:
            The datetime object in GMT time
        '''
        zone = timezone(zonename)
        base = zone.localize(date_time)
        return base.astimezone(GMT)

    @staticmethod
    def __standardize_interval(interval : str):
        if interval[-1] == "m": # Could be 1m, convert it to 1min
            return interval + "in"
        if interval[:-3] == "min": # Everything seems ok
            return interval
        raise ValueError("Invalid interval: {}".format(interval))