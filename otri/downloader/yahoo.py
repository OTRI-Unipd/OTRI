'''
Module containing wrapper classes for Yahoo finance modules.
'''

__author__ = "Luca Crema <lc.crema@hotmail.com>"
__version__ = "3.0"

import html
import json
from datetime import timedelta
from typing import Mapping, Sequence, Union

import yfinance as yf

from ..utils import logger as log
from ..utils import time_handler as th
from . import (DefaultRequestsLimiter, Intervals, MetadataDownloader,
               OptionsDownloader, RequestsLimiter, TimeseriesDownloader)

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
        super().__init__(provider_name=PROVIDER_NAME, intervals=YahooIntervals, limiter=limiter)
        self._set_max_attempts(max_attempts=2)
        self._set_request_timeformat("%Y-%m-%d")
        self._set_aliases(YahooTimeseries.ts_aliases)

    def _history_request(self, ticker: str, start: str, end: str, interval: str = "1m"):
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
        if '/' in ticker:  # Yahoo finance can't handle tickers containing slashes
            return False
        self.limiter._on_request()
        pandas_table = yf.download(tickers=ticker, start=start, end=end, interval=interval, rounding=True, progress=False, prepost=True)
        dictionary = json.loads(pandas_table.to_json(orient="table"))
        self.limiter._on_response()
        return dictionary['data']


class YahooOptions(YahooTimeseries, OptionsDownloader):

    DEFAULT_LIMITER = YahooTimeseries.DEFAULT_LIMITER

    chain_aliases = {
        'contract': 'contractSymbol',
        'last trade datetime': 'lastTradeDate',
        'strike': 'strike',
        'last': 'lastPrice',
        'bid': 'bid',
        'ask': 'ask',
        'volume': 'volume',
        'OI': 'openInterest',
        'IV': 'impliedVolatility',
        'ITM': 'inTheMoney',
        'contract size': 'contractSize',
        'currency': 'currency'
    }

    CONTRACT_SIZES = {
        'REGULAR': 100
    }

    KIND = {
        'call': 'calls',
        'put': 'puts'
    }

    def __init__(self, limiter: RequestsLimiter):
        super().__init__(limiter=limiter)
        self._set_chain_aliases(YahooOptions.chain_aliases)

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
        if '/' in ticker:  # Yahoo finance can't handle tickers containing slashes
            return False
        try:
            tickerObj = yf.Ticker(ticker)
            return list(tickerObj.options)
        except Exception as err:
            log.w("error while loading expiration dates for {}: {}".format(ticker, err))
        return False

    def _chain_request(self, ticker: str, expiration: str, kind: str) -> Union[Sequence[Mapping], bool]:
        '''
        Method that requires data from the provider and transform it into a list of atoms.\n
        It should call the limiter._on_request and limiter._on_response methods if there is anything the limiter needs to know.\n
        Should NOT handle exceptions as they're catched in the superclass.\n

        Parameters:\n
            ticker : str
                Underlying ticker for the option chain\n
            expiration : str
                Expiratio date as string (YYYY-MM-DD).\n
            kind : str
                Either 'call' or 'put'.\n
        '''
        if '/' in ticker:  # Yahoo finance can't handle tickers containing slashes
            return False
        yahoo_kind = YahooOptions.KIND[kind]
        try:
            tick = yf.Ticker(ticker)
            return json.loads(getattr(tick.option_chain(expiration), yahoo_kind).to_json(orient="table"))['data']
        except Exception as err:
            log.w("There has been an error downloading {}: {}".format(ticker, err))
        return False

    def _chain_pre_process(self, chain_atoms: Sequence[Mapping]) -> Sequence[Mapping]:
        for atom in chain_atoms:
            if atom:
                try:
                    atom['lastTradeDate'] = th.datetime_to_str(th.str_to_datetime(atom['lastTradeDate']))
                except KeyError:
                    log.w("unable to find key lastTradeDate in atom")
                try:
                    atom['contractSize'] = YahooOptions.CONTRACT_SIZES[atom['contractSize']]
                except KeyError:
                    log.w("unable to find contract size in {}".format(atom))
        return chain_atoms


class YahooMetadata(MetadataDownloader):
    '''
    Retrieves metadata for tickers.
    '''

    DEFAULT_LIMITER = YahooTimeseries.DEFAULT_LIMITER

    # List of actually valuable pretty static data
    metadata_aliases = {
        "expiration date": "expireDate",
        "algoirthm": "algorithm",
        "dividend rate": "dividendRate",
        "ex dividend rate": "exDividendDate",
        "start date": "startDate",
        "currency": "currency",
        "strike price": "strikePrice",
        "exchange": "exchange",  # PCX, NYQ, NMS
        "short name": "shortName",
        "name": "longName",
        "timezone name": "exchangeTimezoneName",
        "timezone short name": "exchangeTimezoneShortName",
        "symbol type": "quoteType",
        "market": "market",  # us_market
        "full time employees": "fullTimeEmployees",
        "sector": "sector",
        "website": "website",
        "industry": "industry",
        "country": "country",
        "state": "state",
        "isin": "isin"
    }

    def __init__(self, limiter: RequestsLimiter):
        '''
        Parameters:\n
            limiter : RequestsLimiter
                A limiter object, should be shared with other downloaders too in order to work properly.\n
        '''
        super().__init__(provider_name=PROVIDER_NAME, limiter=limiter, max_attempts=2)
        self._set_aliases(YahooMetadata.metadata_aliases)

    def _info_request(self, ticker: str) -> Mapping:
        '''
        Method that requires data from the provider and transform it into a list of atoms.\n
        It should call the limiter._on_request and limiter._on_response methods if there is anything the limiter needs to know.\n
        Should NOT handle exceptions as they're catched in the superclass.\n

        Parameters:\n
            ticker : Sequence[str]
                Symbols to download metadata of.\n
        Returns:
            A list of atoms containing metadata.\n
        '''
        if '/' in ticker:  # Yahoo finance can't handle tickers containing slashes
            return False
        yf_ticker = yf.Ticker(ticker)
        atom = json.loads(html.unescape(json.dumps(yf_ticker.info)))
        isin = yf_ticker.isin
        if isin is not None:
            atom['isin'] = isin
        return atom

    def _post_process(self, atom: Mapping, ticker: str) -> Mapping:
        atom['ticker'] = ticker
        return atom
