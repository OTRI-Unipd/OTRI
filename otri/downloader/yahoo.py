'''
Module containing wrapper classes for Yahoo finance modules.
'''

__author__ = "Luca Crema <lc.crema@hotmail.com>"
__version__ = "4.0"

from datetime import timedelta
from typing import Collection, Dict, List

from ..utils import key_handler as kh
from ..utils import logger as log
from ..utils import time_handler as th
from . import (Adapter, AdapterComponent, DefaultRequestsLimiter,
               ParamValidatorComp, RequestComp, SubAdapter)
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
            buffer = kwargs['buffer']
            output = kwargs['output']
            for data in buffer:
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
                    atom['provider'] = PROVIDER_NAME
                    # Send it to the output
                    output.append(atom)
            buffer.clear()

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
            start: Optional[str]
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


class YahooMetadataAdapter(Adapter):
    '''
    Adapter for yahoo finance metadata.
    '''

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
                atom = {
                    'provider': PROVIDER_NAME,
                    'ticker': real_data['price']['symbol']}
                for key, names in self.metadata_structure.items():
                    for name in names:
                        # Try to get value from data using deep dictionary search passing a string query "name"
                        value = kh.deep_get(real_data[key], name)
                        if value is not None:
                            atom[name.split('.')[0]] = value
                output.append(atom)
            buffer.clear()

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


class YahooOptionsAdapter(Adapter):
    '''
    Adapter for Yahoo Finance options.
    '''

    class YahooOptionsAtomizer(AdapterComponent):

        def compute(self, **kwargs):
            if 'buffer' not in kwargs or 'output' not in kwargs:
                raise ValueError("TradierTimeSeriesAtomizer can only be a retrieval component.")
            if not kwargs['buffer']:
                raise ValueError("Missing data to atomize, data_stream empty")
            data = kwargs['buffer'][0]
            output = kwargs['output']
            if 'optionChain' not in data:
                raise ValueError("Returned data is not of option type")
            if 'result' not in data['optionChain']:
                raise ValueError("Returned data is not of option type")
            if 'options' not in data['optionChain']['result'][0]:
                raise ValueError("Returned data is not of option type")
            options = data['optionChain']['result'][0]['options'][0]
            for call in options['calls']:
                output.append(call)
            for put in options['puts']:
                output.append(put)
            data.clear()

    preparation_components = [
        # Parameter validation
        ParamValidatorComp({
            "date": datetime_param_validation("%Y-%m-%d", required=False)
        }),
        # Datetime (string) to epoch
        DatetimeToEpochComp("date", required=False)
    ]

    retrieval_components = [
        RequestComp(
            base_url=BASE_URL+'v7/finance/options/',
            url_key='ticker',
            query_param_names=['date'],
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
        # Atomization
        YahooOptionsAtomizer()
    ]

    def download(self, ticker: str, expiration_date: str, **kwargs) -> List[Dict]:
        '''
        Parameters:
            ticker : str
                Ticker to download the option chain of.
            expiration_date : str
                A date in %Y-%m-%d format.
        '''
        return super().download(ticker=ticker, date=expiration_date, **kwargs)
