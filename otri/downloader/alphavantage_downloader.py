import json
from datetime import date, datetime
from typing import Union

from alpha_vantage.timeseries import TimeSeries
from pytz import timezone

from ..utils import key_handler as key_handler
from ..utils import time_handler as th
from ..utils import logger as log
from . import (ATOMS_KEY, META_KEY_DOWNLOAD_DT, META_KEY_INTERVAL,
               META_KEY_PROVIDER, META_KEY_TICKER,
               META_KEY_TYPE, META_TS_VALUE_TYPE, METADATA_KEY,
               TimeseriesDownloader)

TIME_ZONE_KEY = "6. Time Zone"
AV_ALIASES = {
    "1. open": "open",
    "2. high": "high",
    "3. low": "low",
    "4. close": "close",
    "5. volume": "volume"
}


class AVTimeseries(TimeseriesDownloader):
    '''
    Used to download historical time series data from AlphaVantage.
    '''

    META_VALUE_PROVIDER = "alpha vantage"

    # Values to round
    FLOAT_KEYS = [
        "open",
        "close",
        "high",
        "low"
    ]

    def __init__(self, api_key: str):
        '''
        Init method.\n
        Parameters:\n
            key : str\n
                the Alpha Vantage API key to use\n
        '''
        self.ts = TimeSeries(api_key, output_format='pandas')

    def history(self, ticker: str, start: date, end: date, interval: str = "1m") -> Union[dict, bool]:
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
        log.d("attempting to download {} for dates: {} to {}".format(ticker, start, end))
        # Interval standardization (eg. 1m to 1min)
        av_interval = AVTimeseries.__standardize_interval(interval)
        try:
            values, meta = self.__call_timeseries_function(
                ticker=ticker, interval=av_interval, start_date=start.replace(day=10))
        except ValueError as exception:
            log.w("AlphaVantage ValueError: {}".format(exception))
            return False
        log.d("successfully downloaded {}".format(ticker))
        # Convert data from pandas dataframe to JSON
        dict_data = json.loads(values.to_json(orient="table"))
        atoms = dict_data['data']
        # Fixing atoms datetime
        atoms = AVTimeseries.__fix_atoms_datetime(
            atoms=atoms, tz=meta[TIME_ZONE_KEY])
        # Renaming keys (removes numbers)
        atoms = key_handler.rename_shallow(atoms, AV_ALIASES)
        # Removing non-requested atoms
        atoms = AVTimeseries.__filter_atoms_by_date(
            atoms=atoms, start_date=start, end_date=end)
        # Rounding too precise numbers
        atoms = key_handler.round_shallow(atoms, AVTimeseries.FLOAT_KEYS)
        # Getting it all together
        data = dict()
        data[ATOMS_KEY] = atoms
        data[METADATA_KEY] = {
            META_KEY_TICKER: ticker,
            META_KEY_INTERVAL: interval,
            META_KEY_PROVIDER: AVTimeseries.META_VALUE_PROVIDER,
            META_KEY_TYPE: META_TS_VALUE_TYPE
        }
        return data

    def __call_timeseries_function(self, start_date: date, interval: str, ticker: str):
        '''
        Calculates the right function to use for the given start date and interval.

        Parameters:
            start_date : date
                Required beginning of data.
            interval : str
                Requied data interval.
            ticker : str
                Required ticker.
        Returns:
            A pandas.Dataframe of required data if successful.
        Raises:
            ValueError: if it couldn't download data.
        '''

        if(interval == "1wk"):
            log.v("required weekly adjusted")
            return self.ts.get_weekly_adjusted(symbol=ticker)
        if(interval == "1d"):
            log.v("required daily adjusted")
            return self.ts.get_daily_adjusted(symbol=ticker, outputsize='full')
        log.v("required intraday")
        return self.ts.get_intraday(symbol=ticker, outputsize='full', interval=interval)

    @staticmethod
    def __filter_atoms_by_date(*, atoms: list, start_date: date, end_date: date) -> list:
        '''
        Trims atoms from the list that don't belong to the interval [start_date, end_date].

        Parameters:
            atoms : list
                List of downloaded atoms.
            start_date : date
                Beginning of required data.
            end_date : date
                End of required data.
        Returns:
            The trimmed list of atoms.
        '''
        required_atoms = list()
        start_datetime = datetime(
            start_date.year, start_date.month, start_date.day, tzinfo=th.local_tzinfo())
        end_datetime = datetime(end_date.year, end_date.month, end_date.day, tzinfo=th.local_tzinfo())

        for atom in atoms:
            atom_datetime = th.str_to_datetime(atom['datetime'])
            if(atom_datetime >= start_datetime and atom_datetime <= end_datetime):
                required_atoms.append(atom)
        return required_atoms

    @staticmethod
    def __fix_atoms_datetime(*, atoms: list, tz: str) -> list:
        '''
        Changes atoms datetime from custom timezone to UTC.
        Also changes datetime dictionary key from "date" to "datetime".

        Parameters:
            atoms : list
                List of un-treated atoms, should contain datetime in "date".
            tz : str
                Current atoms datetime timezone.
        Returns:
            The list of atoms with the correct datetime.
        '''
        for atom in atoms:
            atom["datetime"] = th.datetime_to_str(
                dt=th.str_to_datetime(atom.pop("date"), tz=timezone(tz))
            )
        return atoms

    @ staticmethod
    def __standardize_interval(interval: str) -> str:
        '''
        Standardizes interval format required from Alpha Vantage API.

        Parameters:
            interval : str
                Required interval.
        Returns:
            The interval formatted for Alpha Vantage.
        Raises:
            ValueError: if the interval is not one from the list of possible intervals.
        '''
        if interval[-1] == "m":  # Could be 1m, convert it to 1min
            return interval + "in"
        if interval[:-3] == "min":  # Everything seems ok
            return interval
        if interval != "1d" and interval != "1wk":
            raise ValueError("Invalid interval: {}".format(interval))
        return interval
