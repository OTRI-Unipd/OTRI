'''
Module containing wrapper classes for Yahoo finance modules.
'''

__author__ = "Luca Crema <lc.crema@hotmail.com>"
__version__ = "3.0"

import html
import json
from datetime import date
from typing import Sequence, Union

import yfinance as yf

from ..utils import key_handler as key_handler
from ..utils import logger as log
from ..utils import time_handler as th
from . import (ATOMS_KEY, META_KEY_DOWNLOAD_DT, META_KEY_EXPIRATION,
               META_KEY_INTERVAL, META_KEY_OPTION_TYPE, META_KEY_PROVIDER,
               META_KEY_TICKER, META_KEY_TYPE, META_OPTION_VALUE_TYPE,
               META_TS_VALUE_TYPE, METADATA_KEY, OptionsDownloader,
               TimeseriesDownloader)


class YahooTimeseries(TimeseriesDownloader):
    '''
    Used to download historical time series data from YahooFinance.\n

    Output atom format:
    {
        Open,
        Close,
        High,
        Low,
        Adj Close,
        Volume,
        Datetime,
        provider,
        ticker,
        interval
    }
    '''

    META_VALUE_PROVIDER = "yahoo finance"

    # Values to round
    FLOAT_KEYS = [
        "Open",
        "Close",
        "High",
        "Low",
        "Adj Close"
    ]

    def history(self, ticker: str, start: date, end: date, interval: str = "1m", max_attempts: int = 5) -> Union[dict, bool]:
        '''
        Downloads quote data for a single ticker given two dates.\n

       Parameters:\n
            ticker : str
                The simbol to download data of.\n
            start : date
                Must be before end.\n
            end : date
                Must be after and different from start.\n
            interval : str
                Could be "1m", "2m", "5m", "15m", "30m", "90m", "60m", "1h", "1d", "5d", "1wk"\n
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
        attempts = 0
        while(attempts < max_attempts):
            try:
                # yf_data is type of pandas.Dataframe
                yf_data = yf.download(ticker, start=YahooTimeseries.__yahoo_time_format(start), end=YahooTimeseries.__yahoo_time_format(
                    end), interval=interval, round=False, progress=False, prepost=True)
                break
            except Exception as err:
                attempts += 1
                log.w("there has been an error downloading {} on attempt {}: {}\nTrying again...".format(ticker, attempts, err))

        if(attempts >= max_attempts):
            log.e("unable to download {}".format(ticker))
            return False
        # If no data is downloaded the ticker couldn't be found or there has been an error, we're not creating any output.
        if yf_data is None or yf_data.empty:
            log.w("empty downloaded data {}".format(ticker))
            return False

        return YahooTimeseries.__prepare_data(yf_data, ticker, interval)

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
    def __prepare_data(yf_data, ticker: str, interval: str) -> Union[dict, bool]:
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
            Standardized data dict or False if an error occurred.\n
        '''
        # Conversion from dataframe to dict
        json_data = json.loads(yf_data.to_json(orient="table"))
        # Format datetime and round numeric values
        data = {}
        try:
            rounded_values = key_handler.round_shallow(data=YahooTimeseries.__format_datetime(
                json_data["data"]), keys=YahooTimeseries.FLOAT_KEYS)
        except Exception as e:
            log.w("invalid downloaded data, could not round values: {}".format(e))
            return False
        data[ATOMS_KEY] = rounded_values
        # Addition of metadata
        data[METADATA_KEY] = {
            META_KEY_TICKER: ticker,
            META_KEY_INTERVAL: interval,
            META_KEY_PROVIDER: YahooTimeseries.META_VALUE_PROVIDER,
            META_KEY_TYPE: META_TS_VALUE_TYPE
        }

        return data

    @staticmethod
    def __format_datetime(atoms: list) -> list:
        '''
        Standardizes datetime format.\n

        Parameters:\n
            atoms : list
                list of downloaded atoms, keys must be already lowercased\n
        Returns:\n
            List of atoms with standardized datetime.\n
        '''
        for atom in atoms:
            try:
                atom['Datetime'] = th.datetime_to_str(th.str_to_datetime(atom['Datetime']))
            except KeyError as err:
                log.e("Error in datetime format: {}, atom: {}".format(err, atom))
        return atoms


class YahooOptions(OptionsDownloader):

    META_VALUE_PROVIDER = YahooTimeseries.META_VALUE_PROVIDER

    def expirations(self, ticker: str) -> Union[Sequence[str], bool]:
        '''
        Retrieves the list of expiration dates for option contracts.\n

        Parameters:\n
            ticker : str
                Name of the symbol to get the list of.\n

        Returns:\n
            An ordered sequence of dates as strings of option expiration dates if the download went well,
            False otherwise.
        '''
        log.d("getting list of option expiratiom dates")
        tickerObj = yf.Ticker(ticker)
        # Conversion from tuple to list/sequence
        try:
            return list(tickerObj.options)
        except Exception as err:
            log.w("Error while loading expiration dates for {}: {}".format(ticker, err))
            return False

    def chain(self, ticker: str, expiration: str, kind: str) -> Union[dict, bool]:
        '''
        Retrieves the list of call contracts for the given ticker and expiration date.\n

        Parameters:\n
            ticker : str
                Name of the symbol.\n
            expiration : str
                Expiration date as string, must have been obtained using the get_expiration method.\n
            kind : str
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
                - in the money (true or false)
        '''
        log.d("downloading {} option chain for {} of {}".format(ticker, kind, expiration))
        tick = yf.Ticker(ticker)
        chain = {}
        try:
            # Download option chain for the given kind and remove ["schema"]
            atom_list = json.loads(getattr(tick.option_chain(expiration), kind).to_json(orient="table"))['data']
        except Exception as err:
            log.w("There has been an error downloading {}: {}".format(ticker, err))
            return False

        # Round values and change datetime
        chain[ATOMS_KEY] = key_handler.round_deep(YahooOptions.__format_datetime(atom_list, key="lastTradeDate"))
        # Append medatada values
        chain[METADATA_KEY] = {
            META_KEY_TICKER: ticker,
            META_KEY_PROVIDER: YahooTimeseries.META_VALUE_PROVIDER,
            META_KEY_OPTION_TYPE: kind,
            META_KEY_EXPIRATION: expiration,
            META_KEY_TYPE: META_OPTION_VALUE_TYPE
        }
        return chain

    def chain_contracts(self, ticker: str, expiration: str, kind: str) -> Sequence[str]:
        '''
        Retrives a sequence of contract name/ticker for the given ticker, expiration and type (call or put).\n

        Parameters:\n
            ticker : str
                Name of the symbol.\n
            expiration : str
                Expiration date, must have been obtained using the get_expiration method.\n
            kind : str
                "calls" or "puts"\n

        Returns:\n
            A sequence of contract symbol names (tickers) ordered by the most in the money to the most out of the money.
        '''
        log.d("getting {} list of chain contracts for {} of {}".format(ticker, expiration, kind))
        chain = self.chain(ticker, expiration, kind)
        if chain is False:
            log.w("could not get {} list of chain contract for {} of {}".format(ticker, expiration, kind))
            return []
        symbols = []
        # Extract contract symbols from option chains
        for atom in chain['atoms']:
            symbols.append(atom['contractSymbol'])
        return symbols

    def history(self, contract: str, start: date, end: date, interval: str = "1m") -> Union[dict, bool]:
        '''
        Retrieves a timeseries-like history of a contract.\n

        Parameters:\n
            contract : str
                Name of the contract, usually in the form "ticker"+"date"+"C for calls or P for puts"+"strike price"\n
            start : date
                Must be before end.\n
            end : date
                Must be after and different from start.\n
            interval : str
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
                - volume
        '''
        log.d("downloading contract {} history from {} to {} every {}".format(contract, start, end, interval))
        timeseries_downloader = YahooTimeseries()
        return timeseries_downloader.history(ticker=contract, start=start, end=end, interval=interval, max_attempts=2)

    @staticmethod
    def __format_datetime(atoms: list, key="datetime") -> list:
        '''
        Standardizes datetime format.\n

        Parameters:\n6a
            atoms : list
                list of downloaded atoms, keys must be already lowercased\n
        Returns:\n
            List of atoms with standardized datetime.\n
        '''
        for atom in atoms:
            atom[key] = th.datetime_to_str(th.str_to_datetime(atom[key]))
        return atoms


class YahooMetadata:
    '''
    Retrieves metadata for tickers.
    '''

    ALIASES = {
        "quoteType": META_KEY_TYPE,
        "longName": "name"
    }

    # List of actually valuable pretty static data
    VALUABLE = [
        "expireDate",
        "algorithm",
        "dividendRate",
        "exDividendDate",
        "startDate",
        "currency",
        "strikePrice",
        "exchange",  # PCX, NYQ, NMS
        "shortName",
        "longName",
        "exchangeTimezoneName",
        "exchangeTimezoneShortName",
        "quoteType",
        "market",  # us_market
        "fullTimeEmployees",
        "sector",
        "website",
        "industry",
        "country",
        "state"
    ]

    def info(self, tickers: Sequence[str], max_attempts: int = 2) -> Union[Sequence[dict], bool]:
        '''
        Retrieves the maximum amount of metadata information it can find.\n

        Parameters:\n
            ticker : Sequence[str]
                Identifiers for financial objects.\n
        Returns:\n
            Info as dict if the request went well, False otherwise.
        '''
        data = []
        for ticker in tickers:
            yf_ticker = yf.Ticker(ticker)
            attempts = 0
            while(attempts < max_attempts):
                attempts += 1
                try:
                    yf_info = yf_ticker.info
                    break
                except Exception as e:
                    log.w("There has been an error downloading {} metadata on attempt {}: {}".format(ticker, attempts, e))
                    if str(e) in ("list index out of range", "index 0 is out of bounds for axis 0 with size 0"):
                        continue

            if attempts >= max_attempts:
                continue

            # Remove html entities
            yf_info = json.loads(html.unescape(json.dumps(yf_info)))
            # Filter only valuable keys
            info = {}
            for valuable_key in self.VALUABLE:
                if yf_info.get(valuable_key, None) is not None:
                    info[valuable_key] = yf_info[valuable_key]
            # Rename
            info = key_handler.rename_shallow(info, self.ALIASES)
            # Add ticker
            info['ticker'] = ticker
            # Add isin
            try:
                yf_isin = yf_ticker.isin
                if yf_isin is not None and yf_isin != "-":
                    info['isin'] = yf_isin
            except Exception as e:
                log.e("there has been an exception when retrieving ticker {} ISIN: {}".format(ticker, e))
            # Add provider
            info['provider'] = [YahooTimeseries.META_VALUE_PROVIDER]
            data.append(info)
        return data
