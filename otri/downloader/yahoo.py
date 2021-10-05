'''
Module containing wrapper classes for Yahoo finance modules.
'''

__author__ = "Luca Crema <lc.crema@hotmail.com>"
__version__ = "4.0"

import html
import json
from datetime import timedelta
from typing import Mapping, Sequence, Union, Collection, Dict, List

import yfinance as yf

from ..utils import logger as log
from ..utils import time_handler as th
from ..utils import key_handler as kh
from . import (Adapter, AdapterComponent, DefaultRequestsLimiter, Intervals,
               MetadataDownloader, OptionsDownloader, ParamValidatorComp,
               RequestComp, RequestsLimiter, SubAdapter, TimeseriesDownloader)
from .validators import datetime_param_validation, match_param_validation

PROVIDER_NAME = "yahoo finance"

BASE_URL = "https://query1.finance.yahoo.com/"

INTERVALS = [
    "1m",
    "2m",
    "5m",
    "15m",
    "30m",
    "90m"
    "1h",
    "1d",
    "5d",
    "1wk",
    "1mo",
    "3mo"
]

RANGES = [
    "1d",
    "5d",
    "1mo",
    "3mo",
    "6mo",
    "1y",
    "2y",
    "ytd",
    "max"
]


class YahooIntervals(Intervals):
    ONE_MINUTE = "1m"
    TWO_MINUTES = "2m"
    FIVE_MINUTES = "5m"
    FIFTEEN_MINUTES = "15m"
    THIRTY_MINUTES = "30m"
    ONE_HOUR = "1h"
    ONE_DAY = "1d"


class DatetimeToEpochComp(AdapterComponent):

    def __init__(self, date_key: str, required: bool = True):
        super().__init__()
        self._date_key = date_key
        self._required = required

    def compute(self, **kwargs):
        if self._date_key not in kwargs or not kwargs[self._date_key]:
            if self._required:
                raise ValueError("Missing date_key argument")
            return
        kwargs[self._date_key] = th.datetime_to_epoch(th.str_to_datetime(kwargs[self._date_key]))
        return kwargs


class YahooTimeseriesAdapter(Adapter):
    '''
    Adapter for yahoo finance timeseries data.
    '''

    class YahooTimeseriesAtomizer(AdapterComponent):

        def compute(self, **kwargs):
            if 'buffer' not in kwargs or 'output' not in kwargs:
                raise ValueError("TradierTimeSeriesAtomizer can only be a retrieval component.")
            if not kwargs['buffer']:
                raise ValueError("Missing data to atomize, data_stream empty")
            for data in kwargs['buffer']:
                if data['chart']['error'] != None:
                    raise ValueError(f"Error while downloading yahoo finance data: {data['chart']['error']}")
                elem = data['chart']['result'][0]
                meta = elem['meta']
                quote = elem['indicators']['quote'][0]
                # Zip together the arrays of time and values to create tuples (timestamp, volume, close, ...)
                tuples = zip(elem['timestamp'], quote['volume'], quote['close'],
                             quote['open'], quote['high'], quote['low'])
                for t in tuples:
                    # Transform the tuple to a dict
                    atom = dict(zip(['datetime', 'volume', 'close', 'open', 'high', 'low'], t))
                    # Transform epoch to datetime to str
                    atom['datetime'] = th.datetime_to_str(th.epoch_to_datetime(atom['datetime']))
                    # Append download information
                    atom['ticker'] = meta['symbol']
                    atom['interval'] = meta['dataGranularity']
                    # Send it to the output
                    kwargs['output'].append(atom)
            kwargs['buffer'].clear()

    preparation_components = [
        # Parameter validation
        ParamValidatorComp({
            'interval': match_param_validation(INTERVALS),
            "period1": datetime_param_validation("%Y-%m-%d %H:%M", required=False),
            "period2": datetime_param_validation("%Y-%m-%d %H:%M", required=False),
            'range': match_param_validation(RANGES, required=False)
        }),
        # Datetime (string) to epoch
        DatetimeToEpochComp("period1", required=False),
        DatetimeToEpochComp("period2", required=False)
    ]
    retrieval_components = [
        # Foreach ticker
        SubAdapter(components=[
            RequestComp(
                base_url=BASE_URL+'v8/finance/chart/',
                # Note: yf requires ticker both in the url and in the params eg. /AAPL?symbol=AAPL
                url_key='symbol',
                query_param_names=['symbol', 'interval', 'range', 'period1', 'period2', 'includePrePost'],
                default_header_params={
                        'Accept': 'application/json',
                        'accept-encoding': 'gzip',
                        'user-agent': 'Mozilla/5.0 (Xll; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36'
                },
                default_query_params={
                    'useYfid': 'true'
                },
                to_json=True,
                request_limiter=DefaultRequestsLimiter(requests=4, timespan=timedelta(seconds=1)),
            ),
        ], list_name='tickers', out_name='symbol'),
        # Atomization
        YahooTimeseriesAtomizer()
    ]

    def download(self, tickers: list[str], interval: str, start: str = None, end: str = None, range: str = None, **kwargs) -> List[Dict]:
        '''
        Parameters:
            o_stream: WritableStream
                Output stream for the downloaded data.
            tickers: list[str]
                List of tickers to download the data about.
            interval: Optional[str]
                One of INTERVALS.
                Datetime as string in format %Y-%m-%d %H:%M.
            end: Optional[str]
                Datetime as string in format %Y-%m-%d %H:%M.
            range: Optional[str]
                One of RANGES. Must be used without start and end.
        '''
        return super().download(tickers=tickers,
                                interval=interval,
                                period1=start,
                                period2=end,
                                range=range,
                                **kwargs
                                )


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
        pandas_table = yf.download(tickers=ticker, start=start, end=end, interval=interval,
                                   rounding=True, progress=False, prepost=True)
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


class YahooMetadataAdapter(Adapter):

    class YahooMetadataAtomizer(AdapterComponent):
        '''
        Transforms a yahoo API response into metadata atoms.
        '''

        # List of actually valuable data
        metadata_structure = {
            "price": [
                "exchange",
                "quoteType",
                "shortName",
                "longName",
                "currency"
                "marketCap.raw"  # NOTE: the output atom will only have what is before the point
            ],
            "summaryProfile": [
                "industry",
                "sector",
                "fullTimeEmployees",
                "longBusinessSummary",
                "country",
                "city",
                "state",
            ],
            "defaultKeyStatistics": [
                "enterpriseValue.raw",
                "profitMargins.raw",
                "floatShares.raw",
                "heldPercentInstitutions.raw",
                "shortRatio.raw"
            ]
        }

        def compute(self, **kwargs):
            if 'buffer' not in kwargs or 'output' not in kwargs:
                raise ValueError("TradierTimeSeriesAtomizer can only be a retrieval component.")
            if not kwargs['buffer']:
                raise ValueError("Missing data to atomize, data_stream empty")
            buffer = kwargs['buffer']
            output = kwargs['output']
            for data in buffer:
                real_data = data['quoteSummary']['result'][0]
                atom = {'ticker': real_data['price']['symbol']}
                for key, names in self.metadata_structure.items():
                    for name in names:
                        # Try to get value from data using deep dictionary search passing a string query "name"
                        value = kh.deep_get(real_data[key], name)
                        if value is not None:
                            atom[name.split('.')[0]] = value
                output.append(atom)

    # preparation_components = [] There are no parameters except for ticker list

    retrieval_components = [
        SubAdapter(components=[
            RequestComp(base_url=BASE_URL + "v10/finance/quoteSummary/",
                        url_key="ticker",
                        default_query_params={'modules': 'price,summaryProfile,defaultKeyStatistics'},
                        default_header_params={
                            'Accept': 'application/json',
                            'accept-encoding': 'gzip',
                            'user-agent': 'Mozilla/5.0 (Xll; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36'
                        },
                        to_json=True,
                        request_limiter=DefaultRequestsLimiter(requests=4, timespan=timedelta(seconds=1)),
                        ),
        ], list_name='tickers', out_name='ticker'),
        YahooMetadataAtomizer()
    ]

    def download(self, tickers: Collection[str], **kwargs) -> List[Dict]:
        return super().download(tickers=tickers, **kwargs)
