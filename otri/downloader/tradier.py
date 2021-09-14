'''
Module that contains a wrapper for Tradier.com available data downloading.
'''

__author__ = "Luca Crema <lc.crema@hotmail.com>"
__version__ = "1.0"

from datetime import datetime, timedelta
from typing import Any, List, Mapping, Sequence, Union

import requests
from otri.utils import logger as log
from otri.utils import time_handler as th

from ..filtering.stream import LocalStream, ReadableStream, WritableStream
from . import (Adapter, AdapterComponent, DefaultRequestsLimiter,
               ParamValidatorComp, RealtimeDownloader, RequestComp,
               RequestsLimiter, SubAdapter, TickerChunkComp)

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

INTERVALS = [
    "1min",
    "5min",
    "15min"
]

SESSION_FILTER = [
    "all",
    "open"
]


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
            return super_wait_time
        # If it didn't pass the local maximum amount of requests per minute check the header data
        if(self._available_requests > TradierRequestsLimiter.SAFE_LIMIT):
            return 0
        return (self._next_reset - datetime.utcnow()).total_seconds()


class TradierRealtime(RealtimeDownloader):
    '''
    Downloads realtime data by querying the provider multiple times.
    '''

    # Limiter with pre-setted variables
    DEFAULT_LIMITER = TradierRequestsLimiter(requests=1, timespan=timedelta(seconds=1))

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


class TradierTimeseriesAdapter(Adapter):
    '''
    Synchronous adapter for Tradier timeseries.

    'last' price is the last price of the interval, 'close' is probably the average between ask and bid.
    '''

    class TradierTimeSeriesAtomizer(AdapterComponent):

        def atomize(self, data_stream: ReadableStream, output_stream: WritableStream, **kwargs):
            if not data_stream.has_next():
                raise ValueError("Missing data to atomize, data_stream empty")
            while data_stream.has_next():
                data = data_stream.pop()
                for elem in data['series']['data']:
                    del elem['time']  # delete 'time', redundant
                    elem['datetime'] = th.datetime_to_str(th.epoch_to_datetime(elem['timestamp']))  # convert epoch to UTC datetime
                    del elem['timestamp']  # delete 'timestamp' that was renamed
                    elem['last'] = elem['price']
                    del elem['price']
                    output_stream.append(elem)

    components = [
        # Passed kwargs content validation
        ParamValidatorComp({
            'interval': ParamValidatorComp.match_param_validation(INTERVALS),
            'session_filter': ParamValidatorComp.match_param_validation(SESSION_FILTER, required=False),
            'start': ParamValidatorComp.datetime_param_validation("%Y-%m-%d %H:%M", required=True),
            'end': ParamValidatorComp.datetime_param_validation("%Y-%m-%d %H:%M", required=True)
        }),
        # Foreach ticker
        SubAdapter(components=[
            RequestComp(
                base_url=BASE_URL+'markets/timesales',
                query_param_names=['symbol', 'interval', 'start', 'end', 'session_filter'],
                header_param_names=['Authorization'],
                default_header_params={'Accept': 'application/json'},
                to_json=True,
                request_limiter=TradierRequestsLimiter(requests=1, timespan=timedelta(seconds=1))
            )
        ], list_name='tickers', out_name='symbol'),
        # Atomization
        TradierTimeSeriesAtomizer()
    ]

    def __init__(self, user_key: str):
        super().__init__()
        self._user_key = user_key

    def download(self, o_stream: WritableStream, tickers: list[str], interval: str, start: str, end: str, **kwargs):
        '''
        Parameters:
            o_stream: WritableStream
                Output stream for the downloaded data.
            tickers: list[str]
                List of tickers to download the data about.
            interval: str
                One of INTERVALS.
                Datetime as string in format %Y-%m-%d %H:%M.
            end: str
                Datetime as string in format %Y-%m-%d %H:%M.
            session_filter: Optional[str]
                One of SESSION_FILTER, by default it is 'all'.
        '''
        return super().download(o_stream,
                                tickers=tickers,
                                interval=interval,
                                start=start,
                                end=end,
                                Authorization=f'Bearer {self._user_key}',  # Used in RequestComp headers
                                **kwargs
                                )


class TradierMetadataAdapter(Adapter):
    '''
    Synchronous adapter for Tradier metadata.
    '''

    class TradierMetadataAtomizer(AdapterComponent):

        def atomize(self, data_stream: ReadableStream, output_stream: WritableStream, **kwargs):
            if not data_stream.has_next():
                raise ValueError("Missing data to atomize, data_stream empty")
            while data_stream.has_next():
                data = data_stream.pop()
                if isinstance(data['quotes']['quote'], List):
                    for elem in data['quotes']['quote']:
                        atom = {
                            'ticker': elem['symbol'],
                            'description': elem['description'],
                            'exchange': elem['exch'],
                            'type': elem['type'],
                            'root_symbols': elem['root_symbols'],
                        }
                        output_stream.append(atom)
                else:
                    elem = data['quotes']['quote']
                    atom = {
                        'ticker': elem['symbol'],
                        'description': elem['description'],
                        'exchange': elem['exch'],
                        'type': elem['type'],
                        'root_symbols': elem['root_symbols'],
                    }
                    output_stream.append(atom)

    components = [
        # Ticker splitting from [A, B, C, D] to [[A, B, C], [D]] (although tradier timeseries should only handle 1 ticker at a time)
        TickerChunkComp(max_count=50, tickers_name='tickers', out_name='ticker_groups'),
        # Foreach ticker group eg [[A, B, C], [D]]
        SubAdapter(components=[
            # Foreach ticker list eg. [A, B, C]
            RequestComp(
                base_url=BASE_URL+'markets/quotes',
                query_param_names=['symbols'],
                header_param_names=['Authorization'],
                default_header_params={'Accept': 'application/json'},
                to_json=True,
                request_limiter=TradierRequestsLimiter(requests=1, timespan=timedelta(seconds=1)),
                param_transforms={'symbols': lambda x: ','.join(x)}
            )
        ], list_name='ticker_groups', out_name='symbols'),
        # Atomization
        TradierMetadataAtomizer()
    ]

    def __init__(self, user_key: str):
        super().__init__()
        self._user_key = user_key

    def download(self, o_stream: WritableStream, tickers: list[str], **kwargs) -> LocalStream:
        '''
        Parameters:
            o_stream: WritableStream
                Output stream for the downloaded data.
            tickers: list[str]
                List of tickers to download the data about.
        '''
        return super().download(o_stream,
                                tickers=tickers,
                                Authorization=f'Bearer {self._user_key}',  # Used in RequestComp headers
                                **kwargs
                                )
