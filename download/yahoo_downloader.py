from datetime import date
from download.timeseries_downloader import TimeseriesDownloader
import json
import yfinance as yf

class YahooDownloader(TimeseriesDownloader):
    '''
    
    '''

    def download_between_dates(self, ticker : str, start_date : date, end_date : date, interval : str = "1m"):
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
                Could be "1m" (7 days max); "2m", "5m", "15m", "30m", "90m" (60 days max); "60m", "1h" (730 days max); "1d", "5d", "1wk"
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
        #yf_data is type of pandas.Dataframe
        yf_data = yf.download(ticker, start=YahooDownloader.__yahoo_time_format(start_date), end=YahooDownloader.__yahoo_time_format(end_date), interval=interval, round=False, progress=False, prepost = True)

        # If no data is downloaded it means that the ticker couldn't be found or there has been an error, we're not creating any output file then.
        if yf_data.empty:
            return False
        
        return YahooDownloader.__format_data(yf_data, ticker, interval)

    @staticmethod
    def __yahoo_time_format(date : date):
        '''
        Formats time into yfinance-ready string format for start date and end date.
        Parameters:
            date : datetime
                Datetime to be formatted for yfinance start and end times.
        '''
        return date.strftime("%Y-%m-%d")

    @staticmethod
    def __format_data(yf_data : dict, ticker : str, interval : str):
        json_data = json.loads(yf_data.to_json(orient="table"))
        json_data['atoms'] = json_data.pop("data")
        json_data['metadata'] = {"ticker": ticker, "interval": interval, "provider" : "yahoo finance"}
        del json_data['schema']
        return json_data