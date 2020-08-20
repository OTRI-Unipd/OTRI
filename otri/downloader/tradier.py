'''
Module that contains a wrapper for Tradier.com available data downloading.
'''

__author__ = "Luca Crema <lc.crema@hotmail.com>"
__version__ = "1.0"

import queue
import time
from datetime import date, datetime
from typing import Sequence, Union

import requests

from otri.utils import key_handler
from otri.utils import logger as log
from otri.utils import time_handler as th

from . import (ATOMS_KEY, META_KEY_DOWNLOAD_DT, META_KEY_INTERVAL,
               META_KEY_PROVIDER, META_KEY_TICKER, META_KEY_TYPE,
               META_RT_VALUE_TYPE, META_TS_VALUE_TYPE, METADATA_KEY,
               RealtimeDownloader, TimeseriesDownloader)

BASE_URL = "https://sandbox.tradier.com/v1/"


class TradierTimeseries(TimeseriesDownloader):
    '''
    Download timeseries data one symbol at a time.

    'last' price is the last price of the interval, 'close' is probably the average between ask and bid
    '''

    META_VALUE_PROVIDER = "tradier"

    INTERVALS = {
        "1m": "1min",
        "5m": "5min",
        "15m": "15min"
    }

    FLOAT_KEYS = [
        'price',
        'open',
        'high',
        'low',
        'close',
        'volume',
        'vwap'
    ]

    ALIASES = {
        'price': 'last'
    }

    def __init__(self, api_key: str):
        '''
        Parameters:\n
            api_key : str
                Sandbox user API key.
        '''
        self.key = api_key

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
                Could be "1m", "5m", "15m"\n
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
        fixed_interval = TradierTimeseries.__translate_interval(interval)
        start_str = datetime.combine(start, datetime.min.time()).strftime("%Y-%m-%d %H:%M")
        end_str = datetime.combine(end, datetime.max.time()).strftime("%Y-%m-%d %H:%M")
        result = None
        try:
            result = requests.get(BASE_URL + 'markets/timesales',
                                  params={'symbol': ticker, 'interval': fixed_interval,
                                          'start': start_str, 'end': end_str, 'session_filter': 'all'},
                                  headers={'Authorization': 'Bearer {}'.format(self.key), 'Accept': 'application/json'},
                                  timeout=10
                                  )
        except Exception as e:
            log.e("there has been an error with the request to {} for ticker {}: {}".format(BASE_URL, ticker, e))
            return False

        if result is None:
            return False

        atoms = result.json()['series']['data']
        # Round numeric values
        atoms = key_handler.round_shallow(atoms, TradierTimeseries.FLOAT_KEYS)
        # Fix time and rename it to datetime
        for atom in atoms:
            try:
                atom['datetime'] = th.datetime_to_str(th.str_to_datetime(atom['time']))
                del atom['time']
            except KeyError as err:
                log.e("Error in datetime format: {}, atom: {}".format(err, atom))
        # Rename aliases
        atoms = key_handler.rename_shallow(atoms, aliases=TradierTimeseries.ALIASES)
        # Build output dict
        data = {}
        data[ATOMS_KEY] = atoms
        data[METADATA_KEY] = {
            META_KEY_TICKER: ticker,
            META_KEY_INTERVAL: interval,
            META_KEY_PROVIDER: TradierTimeseries.META_VALUE_PROVIDER,
            META_KEY_TYPE: META_TS_VALUE_TYPE
        }
        return data

    @staticmethod
    def __translate_interval(interval: str):
        return TradierTimeseries.INTERVALS[interval]


class TradierRealtime(RealtimeDownloader):
    '''
    Downloads realtime data by querying the provider multiple times.
    '''

    META_VALUE_PROVIDER = "tradier"

    ALIASES = {
        "symbol": "ticker",
        "exch": "exchange",
        "bidexch": "bid_exchange",
        "askexch": "ask_exchange"
    }

    VALUABLE = [
        "symbol",
        "exch",
        "last",
        "volume",
        "bid",
        "ask",
        "last_volume",
        "trade_date",
        "bidsize",
        "bidexch",
        "bid_date",
        "asksize",
        "askexch",
        "ask_date"
    ]

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

    # Necessary keys to consider the atom valid, if all of them is 0 we can discard the atom
    NECESSARY = [
        "volume",
        "bid",
        "ask",
        "last",
        "last_volume"
    ]

    def __init__(self, key: str):
        '''
        Parameters:\n
            key : str
                Sandbox user key.
        '''
        self.key = key

    def start(self, tickers: Union[str, Sequence[str]], period: float, contents_queue: queue.Queue):
        '''
        Starts the download of the ticker/tickers data.\n

        Parameters:\n
            tickers : Sequence[str]
                List of tickers to download per request. Must be less than 1000 elements.\n
            period : float
                Minimum time between successive requests.\n
            contents_queue : queue.Queue
                Data structure where atoms will be placed asyncronously when downloaded and processed.
        '''
        if isinstance(tickers, str):
            tickers = [tickers]

        str_tickers = self._str_tickers(tickers)
        self.execute = True
        # Start download
        while(self.execute):
            start_time = time.time()
            response = TradierRealtime._require_data(self.key, str_tickers)
            if response is False:
                log.w("request had an error, skipping request")
                continue
            if response.status_code == 200:
                # Prepare data
                processed_response = self.__prepare_data(response.json())
                # Queue data to be uploaded by the uploader thread
                contents_queue.put(processed_response)
            else:
                log.w("Unable to download tickers [{}]: {}".format(str_tickers, response.text))
            # Wait to sync with period
            wait_time = (start_time + period) - time.time()
            if wait_time > 0:
                log.i("Sleeping for {} seconds".format(wait_time))
                time.sleep(wait_time)

    def stop(self):
        '''
        Stops the download of data.
        '''
        self.execute = False

    @staticmethod
    def _require_data(key: str, str_tickers: str, timeout: float = 1.5) -> Union[requests.Response, bool]:
        '''
        Performs an HTTP request to the provider.\n

        Parameters:\n
            key : str
                Sandbox user key.\n
            str_tickers : str
                List of tickers separated by a comma.\n
            timeout : float
                Request timeout time: needed because sometimes the request gets stuck for a long period of time before failing.\n
        Returns:\n
            A requests.Response object if the request went well, False otherwise.
        '''
        try:
            return requests.get(BASE_URL + 'markets/quotes',
                                params={'symbols': str_tickers, 'greeks': 'false'},
                                headers={'Authorization': 'Bearer {}'.format(key), 'Accept': 'application/json'},
                                timeout=timeout
                                )
        except Exception as e:
            log.e("there has been an error with the request to {}: {}".format(BASE_URL, e))
            return False

    @staticmethod
    def __prepare_data(contents: dict) -> dict:
        '''
        Converts downloaded contents into atoms.
        '''
        atoms = contents['quotes']['quote']  # List of atoms
        # Check if it's a single atom
        if isinstance(atoms, dict):
            atoms = [atoms]
        data = {ATOMS_KEY: []}
        for atom in atoms:
            new_atom = {}

            # Check if it's worth keeping
            for key in TradierRealtime.NECESSARY:
                # If any of the necessary keys contains data it's worth keeping
                value = atom.get(key, 0)
                if value != 0 and value is not None:
                    break
            else:
                # log.v("discarding atom: {}".format(atom))
                continue  # discard atom

            # Grab only valuable data
            for key in TradierRealtime.VALUABLE:
                value = atom.get(key, None)
                if value is not None:
                    new_atom[key] = value

            # Localize exch
            for key in ("exch", "bidexch", "askexch"):
                try:
                    new_atom[key] = TradierRealtime.EXCHANGES[new_atom[key]]
                except KeyError:
                    # log.v("unable to localize {}: {}".format(key, new_atom))
                    pass
            # Convert timestamp to datetime
            for key in ("trade_date", "bid_date", "ask_date"):
                if new_atom.get(key, 0) != 0:
                    new_atom[key] = th.datetime_to_str(th.epoc_to_datetime(new_atom[key]/1000))
                else:
                    del new_atom[key]

            # Rename keys
            new_atom = key_handler.rename_shallow(new_atom, TradierRealtime.ALIASES)
            data[ATOMS_KEY].append(new_atom)

        # Append metadata
        data[METADATA_KEY] = {
            META_KEY_PROVIDER: TradierRealtime.META_VALUE_PROVIDER,
            META_KEY_TYPE: META_RT_VALUE_TYPE,
            META_KEY_DOWNLOAD_DT: th.now(),
            META_KEY_INTERVAL: "tick"
        }
        return data

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


class TradierMetadata:
    '''
    Retrieves metadata for tickers.
    '''

    ALIASES = {
        "symbol": "ticker",
        "exch": "exchange"
    }

    VALUABLE = {
        "symbol",
        "exch",
        "type",
        "description",
        "root_symbols"
    }

    def __init__(self, key: str):
        '''
        Parameters:\n
            key : str
                Sandbox user key.
        '''
        self.key = key

    def info(self, tickers: Sequence[str], max_attempts: int = 2) -> Union[Sequence[dict], bool]:
        '''
        Retrieves information for every passed ticker.
        '''
        str_tickers = TradierRealtime._str_tickers(tickers)
        response = TradierRealtime._require_data(self.key, str_tickers)
        if(response in (False, None) or response.status_code != 200):
            return False
        return self.__prepare_data(response.json())

    @staticmethod
    def __prepare_data(contents: dict) -> Union[dict, bool]:
        '''
        Converts downloaded contents into atoms.
        '''
        try:
            atoms = contents['quotes']['quote']  # List of atoms
        except KeyError:
            return False
        if isinstance(atoms, dict):
            atoms = [atoms]
        data = []
        for atom in atoms:
            new_atom = {}
            # Filter
            for key in TradierMetadata.VALUABLE:
                value = atom.get(key, None)
                if value is not None:
                    new_atom[key] = value
            # Localize exchange
            if new_atom.get('exch', None) is not None:
                new_atom['exch'] = TradierRealtime.EXCHANGES[new_atom['exch']]
            # Rename
            new_atom = key_handler.rename_shallow(new_atom, TradierMetadata.ALIASES)

            # Add provider
            new_atom['provider'] = [TradierRealtime.META_VALUE_PROVIDER]
            # Append to output
            data.append(new_atom)
        return data
