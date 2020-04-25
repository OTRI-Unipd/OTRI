'''
Classes:
YFinanceDownloader -- Download all kind of data from Yahoo Finance
'''
from pathlib import Path
from datetime import date, timedelta
from pandas import  DataFrame
import json
import yfinance as yf

class YahooDownloader:
    '''
    Attributes:
        files_directory : pathlib.Path
            Directory where to put JSON output files in.
    
    '''
    def __init__(self, files_directory : Path):
        '''
        Parameters:
        
            files_directory : pathlib.Path
                Existing system directory where to put JSON output files in.
        '''
        self.files_directory = files_directory

    def download_period(self, ticker : str, period : str, interval : str = "1d"):
        '''
        Downloads quote data for a single ticker given a period of time from today.

        Parameters:
            ticker : str
                The simbol to download data of.
            period : str
                Period of time that could be 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max
            interval : str
                Could be "1m" (7 days max); "2m", "5m", "15m", "30m", "90m" (60 days max); "60m", "1h" (730 days max); "1d", "5d", "1wk"
        '''
        yf_data = yf.download(ticker, period=period, interval=interval, round=False, progress=False, prepost = True)

        # If no data is downloaded it means that the ticker couldn't be found or there has been an error, we're not creating any output file then.
        if yf_data.empty:
            return False

        return self.__write_on_JSON_file(YFinanceDownloader.__output_JSON_filename_period(ticker, period), yf_data, ticker, interval)

    def download_dates(self, ticker : str, start_date : date, end_date : date, interval : str = "1d"):
        '''
        Downloads quote data for a single ticker given the start date and end date.

        Parameters:
            ticker : str
                The simbol to download data of.
            start_datetime : datetime
                Must be before end_datetime.
            end_datetime : datetime
                Must be after start_datetime.
            interval : str
                Could be "1m" (7 days max); "2m", "5m", "15m", "30m", "90m" (60 days max); "60m", "1h" (730 days max); "1d", "5d", "1wk"
        '''

        yf_data = yf.download(ticker, start=YFinanceDownloader.__yahoo_time_format(start_date), end=YFinanceDownloader.__yahoo_time_format(end_date), interval=interval, round=False, progress=False, prepost = True)

        # If no data is downloaded it means that the ticker couldn't be found or there has been an error, we're not creating any output file then.
        if yf_data.empty:
            return False

        return self.__write_on_JSON_file(YFinanceDownloader.__output_JSON_filename_dates(ticker, start_date, end_date), yf_data, ticker, interval)

    def download_minimum_interval(self, ticker : str, start_date : date, end_date : date):
        '''
        Downloads quote data for a single ticker given the start date and end date, using the smallest interval
        available for the given dates.

        Parameters:
            ticker : str
                The simbol to download data of.
            start_datetime : datetime
                Must be before end_datetime.
            end_datetime : datetime
                Must be after start_datetime.
        '''
        return self.download_dates(ticker, start_date, end_date, YFinanceDownloader.__calc_minimum_interval(start_date))


    def download_last_week(self, ticker : str):
        '''Downloads quote data for a single ticker a puts it in a beautified JSON file.

        Parameters:
            ticker : str
                The symbol to download data of. Could be with market indication (FTSEMIB) or with market (FTSEMIB.MI).
        '''

        today = date.today()
        seven_days_ago = today - timedelta(days=7)

        return self.download_minimum_interval(ticker, seven_days_ago, today)

    def __write_on_JSON_file(self, json_filename : str, yf_data : DataFrame, ticker_name : str, interval : str):
        '''
        Creates a new file in the files_directory and fills it with tabled Dataframe with the ticker's name added
        Parameters:
            json_filename : str
                Filename where to put data
            yf_data : DataFrame
                Downloaded data with yf.download()
            ticker_name : str
                Ticker name to be added to the output json file
            interval : str
                Interval resolution data has been downloaded
        '''
        json_filepath = Path(self.files_directory, json_filename)
        json_dict = json.loads(yf_data.to_json(orient="table"))
        meta = dict()
        meta['ticker'] = ticker_name
        meta['interval'] = interval
        meta['provider'] = "yahoo finance"
        json_dict['metadata'] = meta
        json_filepath.open("w+").write(json.dumps(json_dict, indent=4))
        return json_filepath

    @staticmethod
    def __yahoo_time_format(date : date):
        '''
        Formats time into yfinance-ready string format for start date and end date.
        Parameters:
            date : datetime.date
                Date to be formatted for yfinance start and end times.
        '''
        return date.strftime("%Y-%m-%d")

    @staticmethod
    def __output_JSON_filename_dates(ticker : str, start_date : date, end_date : date):
        '''
        Generates the JSON output file's name using ticker, data start date and end date.
        Parameters:
            ticker : str
                Output ticker's name.
            start_datetime : datetime
                Beginning date of output data.
            end_datetime : datetime
                End date of output data.
        '''
        return "{}_{}_to_{}.json".format(ticker, start_date.strftime("%d-%m-%y"), end_date.strftime("%d-%m-%y"))

    @staticmethod
    def __output_JSON_filename_period(ticker : str, period : str):
        '''
        Generates the JSON output file's name using ticker and period of time.
        Parameters:
            ticker : str
                Output ticker's name.
            period : str
                Yahoo's period of time
        '''
        return "{}_{}_{}.json".format(ticker, date.today().strftime("%d-%m-%y"), period)

    @staticmethod
    def __calc_minimum_interval(start_date : date):
        '''
        Calculates the shortest interval (from 1min to 1day) allowed for the given interval. For the calculation we only need the start datetime because
        it depends on how old is the wanted data.
        Parameters:
            start_datetime: datetime
                Beginning date of wanted data.
        Returns: The correct shortest yfinance interval
        '''
        days_difference = (date.today() - start_date).days

        if days_difference <= 7:
            return "1m"
        elif days_difference <= 60:
            return "2m"
        elif days_difference <= 730:
            return "1h"
        else:
            return "1d"