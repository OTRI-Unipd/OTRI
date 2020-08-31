'''
Module containing wrapper classes for Yahoo finance modules.
'''

__author__ = "Luca Crema <lc.crema@hotmail.com>"
__version__ = "3.0"

import html
import json
from datetime import date, timedelta
from typing import Sequence, Union

import yfinance as yf

from ..utils import key_handler as key_handler
from ..utils import logger as log
from ..utils import time_handler as th
from . import (ATOMS_KEY, META_KEY_EXPIRATION, META_KEY_OPTION_TYPE, META_KEY_PROVIDER,
               META_KEY_TICKER, META_KEY_TYPE, META_OPTION_VALUE_TYPE, METADATA_KEY, OptionsDownloader,
               TimeseriesDownloader, DefaultRequestsLimiter, Intervals, RequestsLimiter)


PROVIDER_NAME = "yahoo finance"


class YahooIntervals(Intervals):
    ONE_MINUTE = "1m"
    TWO_MINUTES = "2m"
    FIVE_MINUTES = "5m"
    FIFTEEN_MINUTES = "15m"
    THIRTY_MINUTES = "30m"
    ONE_HOUR = "1h"
    ONE_DAY = "1d"


class YahooTimeseries(TimeseriesDownloader):
    '''
    Used to download historical time series data from YahooFinance.\n
    '''

    # Limiter with pre-setted variables
    DEFAULT_LIMITER = DefaultRequestsLimiter(requests=1, timespan=timedelta(milliseconds=200))

    # Expected names for timeseries values
    ts_aliases = {
        'close': 'Close',
        'open': 'Open',
        'high': 'High',
        'low': 'Low',
        'adjusted close': 'Adj Close',
        'volume': 'Volume',
        'datetime': 'Datetime'
    }

    def __init__(self, limiter: RequestsLimiter):
        '''
        Parameters:\n
            limiter : RequestsLimiter
                A limiter object, should be shared with other downloaders too in order to work properly.\n
        '''
        super().__init__(provider_name=PROVIDER_NAME, intervals=YahooIntervals)
        self._set_max_attempts(max_attempts=2)
        self._set_limiter(limiter=limiter)
        self._set_request_timeformat("%Y-%m-%d")
        self._set_aliases(YahooTimeseries.ts_aliases)

    def _request(self, ticker: str, start: str, end: str, interval: str = "1m"):
        '''
        Method that requires data from the provider and transform it into a list of atoms.\n
        Calls limiter._on_request to update the calls made.
        Parameters:\n
            ticker : str
                The simbol to download data of.\n
            start : str
                Download start date.\n
            end : str
                Download end date.\n
            interval : str
                Its possible values depend on the intervals attribute.\n
        '''
        self.limiter._on_request()
        pandas_table = yf.download(tickers=ticker, start=start, end=end, interval=interval, rounding=True, progress=False, prepost=True)
        dictionary = json.loads(pandas_table.to_json(orient="table"))
        return dictionary['data']


class YahooOptions(OptionsDownloader):

    def __init__(self):
        super().__init__(provider_name=PROVIDER_NAME)

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
            META_KEY_PROVIDER: self.provider_name,
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
            info['provider'] = [PROVIDER_NAME]
            data.append(info)
        return data
