'''
Module that contains a wrapper for Tradier.com available data downloading.
'''

__author__ = "Luca Crema <lc.crema@hotmail.com>"
__version__ = "1.0"

from datetime import datetime, timedelta
from typing import Any, Mapping, Sequence, Union

import requests

from otri.utils import logger as log
from otri.utils import time_handler as th

from . import (Intervals, RealtimeDownloader, RequestsLimiter,
               TimeseriesDownloader, MetadataDownloader, DefaultRequestsLimiter)

BASE_URL = "https://sandbox.tradier.com/v1/"

PROVIDER_NAME = "tradier"

EXCHANGES = {
    "A": "NYSE MKT",
    "B": "NASDAQ OMX BX",
    "C": "National Stock Exchange",
    "D": "FINRA ADF",
    "E": "Market Independent(Generated by Nasdaq SIP)",
    "F": "Mutual Funds/Money Markets(NASDAQ)",
    "I": "International Securities Exchange",
    "J": "Direct Edge A",
    "K": "Direct Edge X",
    "M": "Chicago Stock Exchange",
    "N": "NYSE",
    "P": "NYSE Arca",
    "Q": "NASDAQ OMX",
    "S": "NASDAQ Small Cap",
    "T": "NASDAQ Int",
    "U": "OTCBB",
    "V": "OTC other",
    "W": "CBOE",
    "X": "NASDAQ OMX PSX",
    "G": "GLOBEX",
    "Y": "BATS Y-Exchange",
    "Z": "BATS"
}


class TradierIntervals(Intervals):
    ONE_MINUTE = "1min"
    FIVE_MINUTES = "5min"
    FIFTEEN_MINUTES = "15min"


class TradierRequestsLimiter(DefaultRequestsLimiter):
    '''
    Handles tradier requests limitations by reading the response headers and findinding out how many requests are available.
    '''

    # Limit of available requests that the downloader should stop at. Must be >0.
    SAFE_LIMIT = 2

    def __init__(self, requests: int, timespan: timedelta):
        '''
        Parameters:\n
            requests : int
                Number of requests that can be made per timespan.\n
            timespan : timedelta
                Amount of time where the limit is defined.\n
        '''
        super().__init__(requests=requests, timespan=timespan)
        self._available_requests = 5
        self._next_reset = datetime(1, 1, 1)

    def _on_response(self, response_data: Any = None):
        '''
        Called when receiving a response. Updates the requests number.
        '''
        headers = response_data.headers
        self._available_requests = int(headers['X-Ratelimit-Available'])
        self._next_reset = th.epoch_to_datetime(int(headers['X-Ratelimit-Expiry'])/1000)  # In GMT time

    def waiting_time(self):
        '''
        Calculates the amount of time the downloader should wait in order not to exceed provider limitations.\n
        Returns:\n
            The amount of sleep time in seconds. 0 if no sleep time is needed.
        '''
        # First check the local maximum amount of requests
        super_wait_time = super().waiting_time()
        if super_wait_time > 0:
            log.i("passed the {} requests, sleeping for {} seconds".format(self.max_requests, super_wait_time))
            return super_wait_time
        # If it didn't pass the local maximum amount of requests per minute check the header data
        log.i("a:{} reset:{}".format(self._available_requests, th.datetime_to_str(self._next_reset)))
        if(self._available_requests > TradierRequestsLimiter.SAFE_LIMIT):
            return 0
        return (self._next_reset - datetime.utcnow()).total_seconds()


class TradierTimeseries(TimeseriesDownloader):
    '''
    Download timeseries data one symbol at a time.

    'last' price is the last price of the interval, 'close' is probably the average between ask and bid
    '''

    # Limiter with pre-setted variables
    DEFAULT_LIMITER = TradierRequestsLimiter(requests=1, timespan=timedelta(seconds=1))

    ts_aliases = {
        'last': 'price',
        'open': 'open',
        'high': 'high',
        'low': 'low',
        'close': 'close',
        'volume': 'volume',
        'vwap': 'vwap',
        'datetime': 'timestamp'
    }

    def __init__(self, api_key: str, limiter: RequestsLimiter):
        '''
        Parameters:\n
            api_key : str
                Sandbox user API key.\n
            limiter : RequestsLimiter
                A limiter object, should be shared with other downloaders too in order to work properly.\n
        '''
        super().__init__(provider_name=PROVIDER_NAME, intervals=TradierIntervals, limiter=limiter)
        self.key = api_key
        self._set_max_attempts(max_attempts=2)
        self._set_aliases(TradierTimeseries.ts_aliases)
        self._set_datetime_formatter(lambda dt: th.datetime_to_str(dt=th.epoch_to_datetime(dt)))
        self._set_request_timeformat("%Y-%m-%d %H:%M")

    def _history_request(self, ticker: str, start: str, end: str, interval: str = "1min"):
        '''
        Method that requires data from the provider and transform it into a list of atoms.\n
        Calls limiter._on_request to update the calls made.\n

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
        response = self._http_request(ticker=ticker, interval=interval, start=start, end=end, timeout=3)

        if response is None or response is False:
            return False

        self.limiter._on_response(response)

        if response.status_code != 200:
            log.w("Error in Tradier request: {} ".format(str(response.content)))
            return False

        return response.json()['series']['data']

    def _http_request(self, ticker: str, interval: str, start: str, end: str, timeout: float) -> Union[requests.Response, bool]:
        return requests.get(BASE_URL + 'markets/timesales',
                            params={'symbol': ticker, 'interval': interval,
                                    'start': start, 'end': end, 'session_filter': 'all'},
                            headers={'Authorization': 'Bearer {}'.format(self.key), 'Accept': 'application/json'},
                            timeout=timeout
                            )


class TradierRealtime(RealtimeDownloader):
    '''
    Downloads realtime data by querying the provider multiple times.
    '''

    # Limiter with pre-setted variables
    DEFAULT_LIMITER = TradierTimeseries.DEFAULT_LIMITER

    realtime_aliases = {
        'ticker': 'symbol',
        'exchange': 'exch',
        'bid exchange': 'bidexch',
        'ask exchange': 'askexch',
        'last': 'last',
        'volume': 'volume',
        'bid': 'bid',
        'ask': 'ask',
        'last volume': 'last_volume',
        'trade date': 'trade_date',
        'bid size': 'bidsize',
        'last bid date': 'bid_date',
        'ask size': 'asksize',
        'last ask date': 'ask_date'
    }

    def __init__(self, key: str, limiter: RequestsLimiter):
        '''
        Parameters:\n
            key : str
                Sandbox user key.\n
            limiter : RequestsLimiter
                 A limiter object, should be shared with other downloaders too in order to work properly.\n
        '''
        super().__init__(PROVIDER_NAME, limiter)
        self.key = key
        self._set_aliases(TradierRealtime.realtime_aliases)

    def _realtime_request(self, tickers: Sequence[str]) -> Union[Sequence[Mapping], bool]:
        '''
        Method that requires the realtime data from the provider and transforms it into a list of atoms, one per ticker.
        '''
        self.limiter._on_request()
        response = self._http_request(tickers=tickers, timeout=2)

        if response is None or response is False:
            return False

        self.limiter._on_response(response)

        if response.status_code != 200:
            log.w("Error in Tradier request: {} ".format(str(response.content)))
            return False

        return response.json()['quotes']['quote']

    def _post_process(self, atoms: Sequence[Mapping]) -> Sequence[Mapping]:

        for atom in atoms:
            # Exchange localizing
            try:
                for key in ('bid exchange', 'ask exchange', 'exchange'):
                    if atom[key] is not None:
                        atom[key] = EXCHANGES[atom[key]]
            except KeyError as e:
                log.w("error on exchange localization on {}: {}".format(atom, e))
            # Epochs to datetime
            try:
                for key in ('trade date', 'last bid date', 'last ask date'):
                    if atom[key] is not None:
                        atom[key] = th.datetime_to_str(th.epoch_to_datetime(epoch=int(atom[key])/1000))
            except KeyError as e:
                log.w("error on epoch parsing: {}".format(e))
        return atoms

    def _http_request(self, tickers: Sequence[str], timeout: float) -> Union[requests.Response, bool]:
        str_tickers = TradierRealtime._str_tickers(tickers)
        return requests.get(BASE_URL + 'markets/quotes',
                            params={'symbols': str_tickers, 'greeks': 'false'},
                            headers={'Authorization': 'Bearer {}'.format(self.key), 'Accept': 'application/json'},
                            timeout=timeout
                            )

    @staticmethod
    def _str_tickers(tickers: Sequence[str]) -> str:
        '''
        Converts a sequence of tickers into a string with commas separation.
        '''
        str_tickers = ""
        for ticker in tickers:
            ticker = ticker.replace(".", "/")  # Some tickers might be available with the slash instead of dot
            str_tickers += ticker + ","
        str_tickers = str_tickers[:-1]
        return str_tickers


class TradierMetadata(MetadataDownloader):
    '''
    Retrieves metadata for tickers.
    '''

    # Limiter with pre-setted variables
    DEFAULT_LIMITER = TradierTimeseries.DEFAULT_LIMITER

    metadata_aliases = {
        "symbol": "ticker",
        "exchange": "exch",
        "type": "type",
        "description": "description",
        "root_symbols": "root_symbols"
    }

    def __init__(self, key: str, limiter: RequestsLimiter):
        '''
        Parameters:\n
            key : str
                Sandbox user key.\n
            limiter : RequestsLimiter
                A limiter object, should be shared with other downloaders too in order to work properly.\n
        '''
        super().__init__(provider_name=PROVIDER_NAME, limiter=limiter, max_attempts=1)
        self.key = key
        self._set_aliases(TradierMetadata.metadata_aliases)

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
        self.limiter._on_request()
        response = self._http_request(tickers=[ticker], timeout=2)

        if response is None or response is False:
            return False

        self.limiter._on_response(response)

        if response.status_code != 200:
            log.w("Error in Tradier request: {} ".format(str(response.content)))
            return False

        return response.json()['quotes']['quote']

    def _http_request(self, tickers: Sequence[str], timeout: float) -> Union[requests.Response, bool]:
        str_tickers = TradierRealtime._str_tickers(tickers)
        return requests.get(BASE_URL + 'markets/quotes',
                            params={'symbols': str_tickers, 'greeks': 'false'},
                            headers={'Authorization': 'Bearer {}'.format(self.key), 'Accept': 'application/json'},
                            timeout=timeout
                            )
