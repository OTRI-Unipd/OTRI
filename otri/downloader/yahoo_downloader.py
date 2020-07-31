from typing import Union, Sequence
from datetime import date, datetime
from .timeseries_downloader import TimeseriesDownloader, METADATA_KEY, META_INTERVAL_KEY, META_PROVIDER_KEY, META_TICKER_KEY, META_TYPE_KEY, META_TYPE_VALUE as META_TS_VALUE, ATOMS_KEY
from .options_downloader import OptionsDownloader, META_DOWNLOAD_TIME, META_EXPIRATION_DATE, META_TYPE_VALUE as META_OPT_VALUE
from ..utils import key_handler as key_handler
from ..utils import logger as log
from ..utils import time_handler as th
import json
import yfinance as yf

META_PROVIDER_VALUE = "yahoo finance"


class YahooTimeseriesDW(TimeseriesDownloader):
    '''
    Used to download Timeseries data from YahooFinance.
    '''

    def download_between_dates(self, ticker: str, start: date, end: date, interval: str = "1m", max_attempts: int = 5) -> Union[dict, bool]:
        '''
        Downloads quote data for a single ticker given the start date and end date.\n

        Parameters:\n
            ticker : str\n
                The simbol to download data of.\n
            start : date\n
                Must be before end.\n
            end : date\n
                Must be after and different from start.\n
            interval : str\n
                Could be "1m" (7 days max); "2m", "5m", "15m", "30m", "90m" (60 days max); "60m", "1h" (730 days max); "1d", "5d", "1wk"\n
            max_attempts : int\n
                Number of attempts the downloader does when failing before giving up downloading.
        Returns:\n
            False if there as been an error.\n
            A dictionary containing "metadata" and "atoms" otherwise.\n

            "metadata" contains at least:\n
                - ticker\n
                - interval\n
                - provider\n
            "atoms" contains at least:\n
                - datetime (format Y-m-d H:m:s.ms)\n
                - open\n
                - close\n
                - volume\n
        '''
        log.d("attempting to download {}".format(ticker))
        attempts = 0
        while(attempts < max_attempts):
            try:
                # yf_data is type of pandas.Dataframe
                yf_data = yf.download(ticker, start=YahooTimeseriesDW.__yahoo_time_format(start), end=YahooTimeseriesDW.__yahoo_time_format(
                    end), interval=interval, round=False, progress=False, prepost=True)
                break
            except Exception as err:
                attempts += 1
                log.w("There has been an error downloading {} on attempt {}: {}\nTrying again...".format(ticker, attempts, err))

        if(attempts >= max_attempts):
            log.e("unable to download {}".format(ticker))
            return False
        # If no data is downloaded it means that the ticker couldn't be found or there has been an error, we're not creating any output file then.
        if yf_data.empty:
            log.w("empty downloaded data {}".format(ticker))
            return False

        log.d("successfully downloaded {}".format(ticker))
        return YahooTimeseriesDW.__prepare_data(yf_data, ticker, interval)

    @staticmethod
    def __yahoo_time_format(date: date):
        '''
        Formats time into yfinance-ready string format dates.\n
        Parameters:\n
            date : datetime\n
                Datetime to be formatted for yfinance start and end times.\n
        '''
        return date.strftime("%Y-%m-%d")

    @staticmethod
    def __prepare_data(yf_data, ticker: str, interval: str) -> dict:
        '''
        Standardizes timeseries data.\n

        Parameters:\n
            yf_data : pandas.Dataframe\n
                Downloaded historical data.\n
            ticker : str\n
                Name of the downloaded ticker.\n
            interval : str\n
                Amount of time between downloaded atoms.\n
        Returns:\n
            Standardized data dict.\n
        '''
        # Conversion from dataframe to dict
        json_data = json.loads(yf_data.to_json(orient="table"))
        # Format datetime and round numeric values
        data = {}
        data[ATOMS_KEY] = key_handler.round_deep(YahooTimeseriesDW.__format_datetime(json_data["data"]))
        # Addition of metadata
        data[METADATA_KEY] = {
            META_TICKER_KEY: ticker,
            META_INTERVAL_KEY: interval,
            META_PROVIDER_KEY: META_PROVIDER_VALUE,
            META_TYPE_KEY: META_TS_VALUE
        }

        log.v("finished data standardization")
        return data

    @staticmethod
    def __format_datetime(atoms: list) -> list:
        '''
        Standardizes datetime format.\n

        Parameters:\n
            atoms : list\n
                list of downloaded atoms, keys must be already lowercased\n
        Returns:\n
            List of atoms with standardized datetime.\n
        '''
        for atom in atoms:
            try:
                atom['Datetime'] = th.datetime_to_str(datetime.strptime(atom['Datetime'], "%Y-%m-%dT%H:%M:%S.%fZ"))
            except KeyError as err:
                log.e("Error in datetime format: {}, atom: {}".format(err, atom))
        log.v("changed atoms datetime")
        return atoms


class YahooOptionsDW(OptionsDownloader):

    def get_expirations(self, ticker: str) -> Union[Sequence[str], bool]:
        '''
        Retrieves the list of expiration dates for option contracts.\n

        Parameters:\n
            ticker : str\n
                Name of the symbol to get the list of.\n

        Returns:\n
            An ordered sequence of dates as strings of option expiration dates.\n
        '''
        log.d("getting list of option expiratiom dates")
        ticker = yf.Ticker(ticker)
        # Conversion from tuple to list/sequence
        try:
            return list(ticker.options)
        except Exception as err:
            log.w("Error while loading expiration dates for {}: {}".format(ticker, err))
            return False

    def get_chain(self, ticker: str, expiration: str, kind: str) -> Union[dict, bool]:
        '''
        Retrieves the list of call contracts for the given ticker and expiration date.\n

        Parameters:\n
            ticker : str\n
                Name of the symbol.\n
            expiration : str\n
                Expiration date, must have been obtained using the get_expiration method.\n
            kind : str\n
                "calls" or "puts"\n

        Returns:\n
            False if there has been an error.\n
            A dictionary containing "metadata" and "atoms" otherwise.\n

            "metadata" contains at least:\n
                - option type (call / put)\n
                - ticker\n
                - download time\n
                - provider\n
            "atoms" contains at least:\n
                - last trade date (format Y-m-d H:m:s.ms)\n
                - contract symbol\n
                - strike price\n
                - last price\n
                - volume\n
                - in the money (true or false)\n
        '''
        log.d("downloading {} option chain for {} of {}".format(ticker, kind, expiration))
        tick = yf.Ticker(ticker)
        chain = {}
        try:
            # Download option chain for the given kind and remove ["schema"]
            atom_list = json.loads(getattr(tick.option_chain(expiration), kind).to_json(orient="table"))['data']
            # Round values and change datetime
            chain[ATOMS_KEY] = key_handler.round_deep(YahooOptionsDW.__format_datetime(atom_list, key="lastTradeDate"))
        except Exception as err:
            log.w("There has been an error downloading {}: {}".format(ticker, err))
            return False
        # Append medatada values
        chain[METADATA_KEY] = {
            META_TICKER_KEY: ticker,
            META_PROVIDER_KEY: META_PROVIDER_VALUE,
            META_TYPE_KEY: kind,
            META_DOWNLOAD_TIME: th.datetime_to_str(datetime.utcnow()),
            META_EXPIRATION_DATE: expiration,
            META_TYPE_KEY: META_OPT_VALUE
        }
        return chain

    def get_chain_contracts(self, ticker: str, expiration: str, kind: str) -> Sequence[str]:
        '''
        Retrives a sequence of contract name/ticker for the given ticker, expiration and type (call or put).\n

        Parameters:\n
            ticker : str\n
                Name of the symbol.\n
            expiration : str\n
                Expiration date, must have been obtained using the get_expiration method.\n
            kind : str\n
                "calls" or "puts"\n

        Returns:\n
            A sequence of contract symbol names (tickers) ordered by smallest strike price.\n
        '''
        log.d("getting {} list of chain contracts for {} of {}".format(ticker, expiration, kind))
        chain = self.get_chain(ticker, expiration, kind)
        symbols = []
        # Extract contract symbols from option chains
        for atom in chain['atoms']:
            symbols.append(atom['contractSymbol'])
        return symbols

    def get_history(self, contract: str, start: date, end: date, interval: str = "1m") -> Union[dict, bool]:
        '''
        Retrieves a timeseries-like history of a contract.\n

        Parameters:\n
            contract : str\n
                Name of the contract, usually in the form "ticker"+"date"+"C for calls or P for puts"+"strike price"\n
            start : date\n
                Must be before end.\n
            end : date\n
                Must be after and different from start.\n
            interval : str\n
                Frequency for data.\n

        Returns:\n
            False if there as been an error.\n
            A dictionary containing "metadata" and "atoms" otherwise.\n

            "metadata" contains at least:\n
                - ticker\n
                - interval\n
                - provider\n
            "atoms" contains at least:\n
                - datetime (format Y-m-d H:m:s.ms)\n
                - open\n
                - close\n
                - volume\n
        '''
        log.d("downloading contract {} history from {} to {} every {}".format(contract, start, end, interval))
        timeseries_downloader = YahooTimeseriesDW()
        return timeseries_downloader.download_between_dates(ticker=contract, start=start, end=end, interval=interval, max_attempts=2)

    @staticmethod
    def __format_datetime(atoms: list, key="datetime") -> list:
        '''
        Standardizes datetime format.\n

        Parameters:\n
            atoms : list\n
                list of downloaded atoms, keys must be already lowercased\n
        Returns:\n
            List of atoms with standardized datetime.\n
        '''
        for atom in atoms:
            atom[key] = th.datetime_to_str(datetime.strptime(atom[key], "%Y-%m-%dT%H:%M:%S.%fZ"))
        log.v("changed atoms datetime")
        return atoms
