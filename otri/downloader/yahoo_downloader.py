from datetime import date, datetime
from .timeseries_downloader import TimeseriesDownloader, METADATA_KEY, META_INTERVAL_KEY, META_PROVIDER_KEY, META_TICKER_KEY, ATOMS_KEY, Union
from ..utils import key_handler as key_handler
from ..utils import logger as log
import json
import yfinance as yf

META_PROVIDER_VALUE = "yahoo finance"


class YahooDownloader(TimeseriesDownloader):
    '''
    Used to download Timeseries data from YahooFinance.
    '''

    def download_between_dates(self, ticker: str, start: date, end: date, interval: str = "1m") -> Union[dict, bool]:
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
        log.d("attempting to download {}".format(ticker))
        attempts = 0
        while(attempts < 5):
            try:
                # yf_data is type of pandas.Dataframe
                yf_data = yf.download(ticker, start=YahooDownloader.__yahoo_time_format(start), end=YahooDownloader.__yahoo_time_format(
                    end), interval=interval, round=False, progress=False, prepost=True)
                break
            except Exception as err:
                attempts+=1
                log.w("There has been an error downloading {} on attempt {}: {}\nTrying again...".format(ticker, attempts, err))
                
        if(attempts >= 4):
            log.e("unable to download {}".format(ticker))
            return False
        # If no data is downloaded it means that the ticker couldn't be found or there has been an error, we're not creating any output file then.
        if yf_data.empty:
            log.w("empty downloaded data {}".format(ticker))
            return False

        log.d("successfully downloaded {}".format(ticker))
        return YahooDownloader.__prepare_data(yf_data, ticker, interval)

    @staticmethod
    def __yahoo_time_format(date: date):
        '''
        Formats time into yfinance-ready string format dates.
        Parameters:
            date : datetime
                Datetime to be formatted for yfinance start and end times.
        '''
        return date.strftime("%Y-%m-%d")

    @staticmethod
    def __prepare_data(yf_data, ticker: str, interval: str) -> dict:
        '''
        Standardizes timeseries data.

        Parameters:
            yf_data : pandas.Dataframe
                Downloaded historical data.
            ticker : str
                Name of the downloaded ticker.
            interval : str
                Amount of time between downloaded atoms.
        Returns:
            Standardized data dict.
        '''
        # Conversion from dataframe to dict
        json_data = json.loads(yf_data.to_json(orient="table"))
        # Renaming of atoms list
        json_data[ATOMS_KEY] = YahooDownloader.__format_datetime(
            key_handler.lower_all_keys_deep(json_data.pop("data")))
        # Addition of metadata
        json_data[METADATA_KEY] = {
            META_TICKER_KEY: ticker, META_INTERVAL_KEY: interval, META_PROVIDER_KEY: META_PROVIDER_VALUE}
        # Deletion of table headers
        del json_data['schema']

        log.v("finished data standardization")
        return json_data

    @staticmethod
    def __format_datetime(atoms: list) -> list:
        '''
        Standardizes datetime format.

        Parameters:
            atoms : list
                list of downloaded atoms, keys must be already lowercased
        Returns:
            List of atoms with standardized datetime.
        '''
        for atom in atoms:
            atom['datetime'] = datetime.strptime(atom['datetime'], "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        log.v("changed atoms datetime")
        return atoms
