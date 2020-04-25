from datetime import datetime

class TimeseriesDownloader:
    '''
    Abstract class that defines any type of data downloading from any source of time series.
    '''

    def download_period(self, ticker : str, period : str, interval : str):
        '''
        Downloads quote data for a single ticker given a period of time from today.

        Parameters:
            ticker : str
                The simbol to download data of.
            period : str
                Period of time that could be something like "1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max"
            interval : str
                Could be "1m" (7 days max); "2m", "5m", "15m", "30m", "90m", "60m", "1h", "1d", "5d", "1wk"
        Returns:
            A dict of intraday data containing
        '''
        raise NotImplementedError("This is an abstract class, please implement this method in a child class")

    def download_between_dates(self, ticker : str, start : datetime, end : datetime, interval : str):
        '''
        Downloads quote data for a single ticker given two dates.

        Parameters:
            ticker : str
                The simbol to download data of.
            start_datetime : datetime
                Beginning datetime for data download.
            end_datetime : datetime
                End datetime for data download.
            interval : str
                Could be "1m" (7 days max); "2m", "5m", "15m", "30m", "90m", "60m", "1h", "1d", "5d", "1wk"
        Returns:
            A dict containing "metadata" and "atoms".

            metadata is a dict containing at least:
                - ticker
                - interval
                - provider
                - other data that the atomizer could want to apply to every atom
            
            atoms is a list of dicts containing:
                - datetime (format Y-m-d H:m:s.ms)
                - other financial values
        '''