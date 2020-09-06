import json
from datetime import timedelta
from typing import Mapping, Sequence

from alpha_vantage.timeseries import TimeSeries
from pytz import timezone

from ..utils import logger as log
from ..utils import time_handler as th
from . import (DefaultRequestsLimiter, Intervals, RequestsLimiter,
               TimeseriesDownloader)

TIME_ZONE_KEY = "6. Time Zone"

PROVIDER_NAME = "alpha vantage"


class AVIntervals(Intervals):
    ONE_MINUTE = "1min"
    FIVE_MINUTES = "5min"
    FIFTEEN_MINUTES = "15min"
    THIRTY_MINUTES = "30min"
    ONE_HOUR = "60min"


class AVTimeseries(TimeseriesDownloader):
    '''
    Used to download historical time series data from AlphaVantage.
    '''

    # Limiter with pre-setted variables
    DEFAULT_LIMITER = DefaultRequestsLimiter(requests=5, timespan=timedelta(minutes=1))

    ts_aliases = {
        'close': '4. close',
        'open': '1. open',
        'high': '2. high',
        'low': '3. low',
        'volume': '5. volume',
        'datetime': 'date'
    }

    def __init__(self, api_key: str, limiter: RequestsLimiter):
        '''
        Parameters:\n
            key : str
                An Alpha Vantage user API key.\n
            limiter : RequestsLimiter
                A limiter object, should be shared with other downloaders too in order to work properly.\n
        '''
        super().__init__(provider_name=PROVIDER_NAME, intervals=AVIntervals, limiter=limiter)
        self.ts = TimeSeries(api_key, output_format='pandas')
        self._set_max_attempts(max_attempts=1)
        self._set_aliases(AVTimeseries.ts_aliases)
        self._set_datetime_formatter(lambda dt: th.datetime_to_str(dt=th.str_to_datetime(dt, tz=self._cur_timezone)))

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
        values, metadata = self.ts.get_intraday(symbol=ticker, outputsize='full', interval=interval)
        dictionary = json.loads(values.to_json(orient="table"))
        # Retrieve timezone from metadata before losing it
        try:
            self._cur_timezone = timezone(metadata[TIME_ZONE_KEY])
        except KeyError:
            log.w("missing timezone definition, assuming UTC")
            self._cur_timezone = timezone('UTC')
        # Return atoms
        return dictionary['data']

    def _post_process(self, atoms: Sequence[Mapping], **kwargs) -> Sequence[Mapping]:
        '''
        Optional method to further process atoms after all the standard processes like aliasing and date formatting.\n

        Parameters:\n
            atoms : Sequence[Mapping]
                atoms downloaded and alised.\n
            kwargs
                Anything that the caller function can pass.\n
        '''
        required_atoms = list()
        start = kwargs['start']
        end = kwargs['end']
        if start.tzinfo is None:
            start = start.replace(tzinfo=th.local_tzinfo())
        if end.tzinfo is None:
            end = end.replace(tzinfo=th.local_tzinfo())
        for atom in atoms:
            atom_datetime = th.str_to_datetime(atom['datetime'])
            if(atom_datetime >= start and atom_datetime <= end):
                required_atoms.append(atom)
        return required_atoms
