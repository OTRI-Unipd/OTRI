'''
Module containing wrapper classes for Yahoo finance modules.
'''

__author__ = "Luca Crema <lc.crema@hotmail.com>"
__version__ = "3.0"

import html
import json
from datetime import timedelta
from typing import Sequence, Union, Mapping

import yfinance as yf

from ..utils import key_handler as key_handler
from ..utils import logger as log
from ..utils import time_handler as th
from . import (META_KEY_TYPE, OptionsDownloader,
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
        self.limiter._on_request()
        pandas_table = yf.download(tickers=ticker, start=start, end=end, interval=interval, rounding=True, progress=False, prepost=True)
        dictionary = json.loads(pandas_table.to_json(orient="table"))
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
