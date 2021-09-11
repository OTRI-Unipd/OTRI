
__author__ = "Luca Crema <lc.crema@hotmail.com>, Riccardo De Zen <riccardodezen98@gmail.com>"
__version__ = "2.0"

import traceback
from datetime import date, datetime, timedelta, time, timezone as tz
from queue import Queue
from time import sleep
from typing import Any, Callable, Mapping, Sequence, Union, Set, List
from collections.abc import Iterable
from abc import ABC, abstractmethod
import requests

from ..utils import logger as log
from ..utils import time_handler as th
from ..filtering.stream import Stream, LocalStream, WritableStream, ReadableStream

# All downloaders
ATOMS_KEY = "atoms"
METADATA_KEY = "metadata"
META_KEY_TICKER = "ticker"
META_KEY_TYPE = "type"
META_KEY_PROVIDER = "provider"
META_KEY_DOWNLOAD_DT = "download datetime"

# Timeseries downloader
META_KEY_INTERVAL = "interval"
META_TS_VALUE_TYPE = "price"

# Options downloader
META_KEY_EXPIRATION = "expiration"
META_KEY_OPTION_TYPE = "option type"
META_OPTION_VALUE_TYPE = "option"

# Realtime trade downloader
META_RT_VALUE_TYPE = "trade"


class Intervals:
    ONE_MINUTE = None
    TWO_MINUTES = None
    FIVE_MINUTES = None
    TEN_MINUTES = None
    FIFTEEN_MINUTES = None
    THIRTY_MINUTES = None
    ONE_HOUR = None
    ONE_DAY = None


class RequestsLimiter:
    '''
    Object that handles the provider requests limitations.
    Could be as simple as the DefaultRequestsLimiter or something more complex that uses something in the request or response.\n
    Must be thread safe.
    '''

    def waiting_time(self):
        '''
        Calculates the amount of time the downloader should wait in order not to exceed provider limitations.\n
        Returns:\n
            The amount of sleep time in seconds. 0 if no sleep time is needed.
        '''
        raise NotImplementedError("waiting_time is an abstract method, please implement it in a class")

    def _on_request(self, request_data: Any = None):
        '''
        Called by the downloader when performing a request.\n
        Parameters:\n
            request_data : Any
                Some kind of data the downloader might want to pass to the limiter for its calculations.\n
        '''
        pass

    def _on_response(self, response_data: Any = None):
        '''
        Called by the downloader when receiving a response.\n
        Parameters:\n
            response_data : Any
                Some kind of data the downloader might want to pass to the limiter for its calculations.\n
        '''
        pass


class DefaultRequestsLimiter(RequestsLimiter):
    '''
    Handles the provider requests limitations by setting a maximum request amount per timedelta (minutes, hours, days, ...).
    '''

    def __init__(self, requests: int, timespan: timedelta):
        '''
        Parameters:\n
            requests : int
                Number of requests that can be made per timespan.\n
            timespan : timedelta
                Amount of time where the limit is defined.\n
        '''
        self.max_requests = requests
        self.timespan = timespan
        self.next_reset = datetime(2000, 1, 1)
        self.request_counter = 0

    def _on_request(self, request_data: Any = None):
        '''
        Called when performing a request. Updates the requests number.
        '''
        # If enough time has passed we can reset the counter.
        if(datetime.utcnow() > self.next_reset):
            self.next_reset = datetime.utcnow() + self.timespan
            self.request_counter = 0
        # Update the counter
        self.request_counter += 1

    def waiting_time(self):
        '''
        Calculates the amount of time the downloader should wait in order not to exceed provider limitations.\n
        Returns:\n
            The amount of sleep time in seconds. 0 if no sleep time is needed.
        '''
        if(self.request_counter < self.max_requests):
            return 0
        elif(datetime.utcnow() < self.next_reset):
            return (self.next_reset - datetime.utcnow()).total_seconds()
        else:
            return 0


class Downloader:
    '''
    Defines an interface with a data provider of any kind.
    '''

    aliases = {
        'datetime': None
    }

    # Default class limiter, can be used to avoid keeping track of provider specific parameters.
    DEFAULT_LIMITER = RequestsLimiter()

    def __init__(self, provider_name: str, limiter:  RequestsLimiter):
        '''
        Parameters:\n
            provider_name : str
                Name of the provider, will be used when storing data in the db.\n
            limiter : RequestsLimiter
                A limiter object, should be shared with other downloaders too in order to work properly.\n
        '''
        self.provider_name = provider_name
        self.limiter = limiter
        self.max_attempts = 1

    def _set_aliases(self, aliases: Mapping[str, str]):
        '''
        Extends the current aliases dictionary with new aliases overriding current ones.\n
        Used to filter and rename fields in the downloaded data.\n

        Parameters:\n
            aliases : Mapping[str, str]
                Key-value pairs that define the renaming of atoms' keys. Values must be all lowecased.\n
        '''
        self.aliases.update(aliases)

    def _set_max_attempts(self, max_attempts: int):
        '''
        Parameters:\n
            max_attempts : int
                Number of maximum attempts the downloader will do to download data. Does not include the data elaboration,
                if something goes wrong when working on downloaded data the script won't attempt to download it again.\n
        '''
        self.max_attempts = max_attempts

    def _set_datetime_formatter(self, formatter: Callable):
        '''
        Sets a different datetime formatter for atoms than the default one.

        Parameters:
            datetime_formatter : str
                Method that takes a datetime string as parameter and returns a properly formatted YYYY-MM-DD HH:mm:ss.fff datetime string.\n
        '''
        self.datetime_formatter = formatter


class TimeseriesDownloader(Downloader):
    '''
    Defines historical time series data downloading.\n
    The download should be performed only once and not continuosly.
    '''

    aliases = {
        'close': None,
        'open': None,
        'high': None,
        'low': None,
        'adjusted close': None,
        'volume': None,
        'datetime': None
    }

    def __init__(self, provider_name: str, intervals: Intervals, limiter:  RequestsLimiter, max_attempts: int = 2):
        '''
        Parameters:\n
            provider_name : str
                Name of the provider, will be used when storing data in the db.\n
            intervals : Intervals
                Defines supported intervals and their aliases for the request. It should extend the otri.downloader.Intervals class.\n
            limiter : RequestsLimiter
                A limiter object, should be shared with other downloaders too in order to work properly.\n
            max_attempts : int
                Maximum attempts to download historical data.\n
        '''
        super().__init__(provider_name=provider_name, limiter=limiter)
        self._set_max_attempts(max_attempts)
        self.intervals = intervals
        self.request_dateformat = "%Y-%m-%d %H:%M"
        self.datetime_formatter = lambda dt: th.datetime_to_str(th.str_to_datetime(dt))

    def history(self, ticker: str, start: datetime, end: datetime, interval: Intervals) -> Union[dict, bool]:
        '''
        Downloads time-series data for a single ticker given two dates.\n

       Parameters:\n
            ticker : str
                The symbol to download data of.\n
            start : datetime
                Must be before end.\n
            end : datetime
                Must be after and different from start.\n
            interval : Intervals
                Can be an enum from any class that extends Intervals. See 'intervals' attribute for possible values.\n
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
                - high\n
                - low\n
                - close\n
                - volume\n
        '''
        if interval is None:
            raise Exception(f"Interval not supported by {self.provider_name}")

        data = dict()
        # Attempt to download and parse data a number of times that is max_attempts
        attempts = 0
        while(attempts < self.max_attempts):
            try:
                # Check if there's any wait time to do
                wait_time = self.limiter.waiting_time()
                while wait_time > 0:
                    sleep(wait_time)
                    wait_time = self.limiter.waiting_time()

                # Request data as a list of atoms
                atom_list = self._history_request(ticker=ticker, start=start.strftime(self.request_dateformat),
                                                  end=end.strftime(self.request_dateformat), interval=interval)
                break
            except Exception as err:
                attempts += 1
                log.w("error downloading {} on attempt {}: {}".format(ticker, attempts, err))
                # log.v(traceback.format_exc())
        else:
            # It reached the maximum number of attempts (while did not break)
            log.e("giving up download of {}, reached max attempts".format(ticker))
            return False

        # If no data is downloaded the ticker couldn't be found or there has been an error, we're not creating any output.
        if atom_list is None or not atom_list:
            log.w("empty downloaded data {}: {}".format(ticker, atom_list))
            return False

        # Optional atoms preprocessing
        preprocessed_atoms = self._pre_process(atoms=atom_list, start=start, end=end, interval=interval, ticker=ticker)
        # Process atoms keys using aliases and datetime formatter
        prepared_atoms = []
        for atom in preprocessed_atoms:
            new_atom = {}
            # Renaming and filtering fields
            for key, value in self.aliases.items():
                if value is not None and value in atom:
                    try:
                        new_atom[key] = atom[value]
                    except Exception as e:
                        log.w("Exception thrown on renaming atom: {}. Exception: {}. Ticker: {} Preprocessed atoms: {}".format(
                            atom, e, ticker, preprocessed_atoms))
            # Datetime formatting
            try:
                new_atom['datetime'] = self.datetime_formatter(new_atom['datetime'])
            except KeyError:
                log.w("missing atoms datetime: {}".format(new_atom))
                continue  # Avoid saving atom, without a datetime it's useless
            if not new_atom:
                log.w("empty atom, nothing aliased: {}".format(atom))
                continue

            prepared_atoms.append(new_atom)

        # Further optional subclass processing
        postprocessed_atoms = self._post_process(
            atoms=prepared_atoms, start=start, end=end, interval=interval, ticker=ticker)
        # Append atoms to the output
        data[ATOMS_KEY] = postprocessed_atoms
        # Create metadata and append it to the output
        data[METADATA_KEY] = {
            META_KEY_TICKER: ticker,
            META_KEY_INTERVAL: interval,
            META_KEY_PROVIDER: self.provider_name,
            META_KEY_TYPE: META_TS_VALUE_TYPE
        }
        return data

    def _set_request_timeformat(self, request_dateformat: str):
        '''
        By default request timeformat is '%Y-%m-%d %H:%M'.\n

        Parameters:
            request_dateformat : str
                String format passed to datetime.strftime before giving the date to the request method.\n
        '''
        self.request_dateformat = request_dateformat

    def _history_request(self, ticker: str, start: date, end: date, interval: str) -> list:
        '''
        Method that requires data from the provider and transform it into a list of atoms.\n
        It should call the limiter._on_request and limiter._on_response methods if there is anything the limiter needs to know.\n
        Should NOT handle exceptions as they're catched in the superclass.\n

        Parameters:\n
            ticker : str
                The symbol to download data of.\n
            start : date
                Must be before end.\n
            end : date
                Must be after and different from start.\n
            interval : str
                Its possible values depend on the intervals attribute.\n
        '''
        raise NotImplementedError("_history_request is an abstract method, please implement it in a class")

    def _pre_process(self, atoms: Sequence[Mapping], **kwargs) -> Sequence[Mapping]:
        '''
        Optional metod to pre-process data before aliasing and date formatting.\n
        Atoms processing should be done here rather than in request because if it fails it won't try another attempt,
        because the error is not in the download but in the processing.\n

        Parameters:\n
            atoms : Sequence[Mapping]
                Atoms downloaded and aliased.\n
            kwargs
                Anything that the caller function can pass.\n
        '''
        return atoms

    def _post_process(self, atoms: Sequence[Mapping], **kwargs) -> Sequence[Mapping]:
        '''
        Optional method to further process atoms after all the standard processes like aliasing and date formatting.\n

        Parameters:\n
            atoms : Sequence[Mapping]
                Atoms downloaded and aliased.\n
            kwargs
                Anything that the caller function can pass.\n
        '''
        return atoms


class OptionsDownloader(TimeseriesDownloader):
    '''
    Abstract class that defines downloading of options chain, "contracts" history, bids and asks.\n
    The download should be performed only once and not continuosly.
    '''

    chain_aliases = {
        'bid': None,
        'ask': None,
        'OI': None,
        'IV': None,
        'ITM': None,
        'strike': None,
        'contract': None
    }

    def __init__(self, provider_name: str, intervals: Intervals, limiter:  RequestsLimiter, max_attempts: int = 2, chain_max_attempts: int = 2):
        '''
        Parameters:\n
            provider_name : str
                Name of the provider, will be used when storing data in the db.\n
            intervals : Intervals
                Defines supported intervals and their aliases for the request. It should extend the otri.downloader.Intervals class.\n
            limiter : RequestsLimiter
                A limiter object, should be shared with other downloaders too in order to work properly.\n
            max_attempts : int
                Maximum attempts to download historical data.\n
            chain_max_attempts : int
                Maximum attempts to download option chain data.\n
        '''
        super().__init__(provider_name=provider_name, intervals=intervals, limiter=limiter, max_attempts=max_attempts)
        self._set_chain_max_attempts(chain_max_attempts)

    def expirations(self, ticker: str) -> Union[Sequence[str], bool]:
        '''
        Retrieves the list of expiration dates for option contracts.\n

        Parameters:\n
            ticker : str
                Name of the symbol to get the list of.\n

        Returns:\n
            An ordered sequence of dates as strings of option expiration datesif the download went well,
            False otherwise.
        '''
        raise NotImplementedError("expirations is an abstract method, please implement it in a class")

    def chain(self, ticker: str, expiration: str, kind: str) -> Union[Mapping, bool]:
        '''
        Retrieves the list of call contracts for the given ticker and expiration date.\n

        Parameters:\n
            ticker : str
                Underlying ticker for the option chain.\n
            expiration : str
                Expiration date as string, must have been obtained using the get_expiration method.\n
            kind : str
                "call" or "put"\n

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
        data = dict()

        # Attempt to download and parse data a number of times that is chain_max_attempts
        attempts = 0
        while(attempts < self.chain_max_attempts):
            # Check if there's any wait time to do
            wait_time = self.limiter.waiting_time()
            while wait_time > 0:
                sleep(wait_time)
                wait_time = self.limiter.waiting_time()
            try:
                # Request data as a list of atoms
                atom_list = self._chain_request(ticker=ticker, expiration=expiration, kind=kind)
                break
            except Exception as err:
                attempts += 1
                log.w("error downloading {} option chain on attempt {}: {}".format(ticker, attempts, err))
                # log.v(traceback.format_exc())
        else:
            # It reached the maximum number of attempts (while did not break)
            log.e("giving up download of {}, reached max attempts".format(ticker))
            return False

        # If no data is downloaded the ticker couldn't be found or there has been an error, we're not creating any output.
        if atom_list is None or not atom_list:
            log.w("empty downloaded data {}: {}".format(ticker, atom_list))
            return False

        # Optional atoms preprocessing
        preprocessed_atoms = self._chain_pre_process(chain_atoms=atom_list)
        # Process atoms keys using aliases and datetime formatter
        prepared_atoms = []
        for atom in preprocessed_atoms:
            new_atom = {}
            # Renaming and fields filtering
            for key, value in self.chain_aliases.items():
                if value is not None and value in atom:
                    try:
                        new_atom[key] = atom[value]
                    except Exception as e:
                        log.w("Exception thrown on renaming atom: {}. Exception: {}. Ticker: {} Preprocessed atoms: {}".format(
                            atom, e, ticker, preprocessed_atoms))
            if not new_atom:
                log.w("empty atom, nothing aliased: {}".format(atom))
                continue

            prepared_atoms.append(new_atom)

        # Further optional subclass processing
        postprocessed_atoms = self._chain_post_process(chain_atoms=prepared_atoms)
        # Append atoms to the output
        data[ATOMS_KEY] = postprocessed_atoms
        # Create metadata and append it to the output
        data[METADATA_KEY] = {
            META_KEY_TICKER: ticker,
            META_KEY_PROVIDER: self.provider_name,
            META_KEY_OPTION_TYPE: kind,
            META_KEY_EXPIRATION: expiration,
            META_KEY_TYPE: META_OPTION_VALUE_TYPE
        }

        return data

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
                Either 'calls' or 'puts'.\n
        '''
        raise NotImplementedError("_chain_request is an abstract method, please implement it in a class")

    def _chain_pre_process(self, chain_atoms: Sequence[Mapping]) -> Sequence[Mapping]:
        return chain_atoms

    def _chain_post_process(self, chain_atoms: Sequence[Mapping]) -> Sequence[Mapping]:
        return chain_atoms

    def _set_chain_aliases(self, chain_aliases: Mapping[str, str]):
        '''
        Extends the current chain_aliases dictionary with new aliases overriding current ones.\n
        Used to filter and rename fields in the downloaded chain data.\n

        Parameters:\n
            chain_aliases : Mapping[str, str]
                Key-value pairs that define the renaming of atoms' keys. Values must be all lowecased.\n
        '''
        self.chain_aliases.update(chain_aliases)

    def _set_chain_max_attempts(self, max_attempts: int):
        '''
        Parameters:\n
            max_attempts : int
                Number of maximum attempts the downloader will do to download chain data. Does not include the data elaboration,
                if something goes wrong when working on downloaded data the script won't attempt to download it again.\n
        '''
        self.chain_max_attempts = max_attempts


class RealtimeDownloader(Downloader):
    '''
    Abstract class that defines a continuous download of a single atom per ticker by sending multiple requests to the provider.\n
    For streaming see StreamingDownloader (Not implemented yet).\n
    '''

    realtime_aliases = {
        'last': None,
        'last volume': None
    }

    def __init__(self, provider_name: str, limiter:  RequestsLimiter):
        '''
        Parameters:\n
            provider_name : str
                Name of the provider, will be used when storing data in the db.\n
            limiter : RequestsLimiter
                A limiter object, should be shared with other downloaders too in order to work properly.\n
        '''
        super().__init__(provider_name=provider_name, limiter=limiter)
        self.execute = False
        self.working_hours = {'start': time(hour=10, minute=00, tzinfo=tz.utc),
                              'stop': time(hour=23, minute=59, tzinfo=tz.utc)}

    def start(self, tickers: Union[str, Sequence[str]], contents_queue: Queue):
        '''
        Starts the download of the ticker/tickers data.\n

        Parameters:\n
            tickers : Sequence[str]
                List of tickers to download per request. Must be shorter than 1000 elements.\n
            period : float
                Minimum time between successive requests.\n
            contents_queue : queue.Queue
                Data structure where atoms will be placed asyncronously when downloaded and processed.
        '''
        self.execute = True
        if isinstance(tickers, str):
            tickers = [tickers]
        while(self.execute):
            # Check if it's working hours
            now_time = datetime.utcnow().replace(tzinfo=tz.utc).timetz()
            if(now_time < self.working_hours['start'] or now_time > self.working_hours['stop']):
                diff_to_start = th.sub_times(self.working_hours['start'], now_time)
                if diff_to_start < 0:
                    diff_to_start = 86400 + diff_to_start  # Rocking around the clock
                log.i("going to sleep until start time: {} seconds".format(diff_to_start))
                sleep(diff_to_start)

            # Wait if too frequent requests are being made
            wait_time = self.limiter.waiting_time()
            while wait_time > 0:
                sleep(wait_time)
                wait_time = self.limiter.waiting_time()
            # Download raw data
            try:
                atom_list = self._realtime_request(tickers=tickers)
            except Exception as err:
                log.w("error downloading realtime data: {}. Traceback: {}".format(err, traceback.format_exc()))
                continue
            # Check data
            if atom_list is None or atom_list is False:
                log.w("empty downloaded data: {}".format(atom_list))
                continue
            # Pre-process
            preprocessed_atoms = self._pre_process(atom_list)
            # Actual process
            prepared_atoms = []
            for atom in preprocessed_atoms:
                new_atom = {}
                # Aliasing and fields filtering
                for key, value in self.aliases.items():
                    if value is not None and value in atom:
                        try:
                            new_atom[key] = atom[value]
                        except Exception as e:
                            log.w("Exception thrown on renaming atom {}: {}".format(atom, e))
                if not new_atom:
                    log.w("empty atom, nothing aliased: {}".format(atom))
                    continue

                prepared_atoms.append(new_atom)

            # Further optional subclass processing
            postprocessed_atoms = self._post_process(atoms=prepared_atoms)
            # Append atoms to the output
            data = dict()
            data[ATOMS_KEY] = postprocessed_atoms
            # Create metadata and append it to the output
            data[METADATA_KEY] = {
                META_KEY_INTERVAL: "tick",
                META_KEY_PROVIDER: self.provider_name,
                META_KEY_TYPE: META_RT_VALUE_TYPE
            }
            contents_queue.put({'data': data})

    def _realtime_request(self, tickers: Sequence[str]) -> Union[Sequence[Mapping], bool]:
        '''
        Method that requires the realtime data from the provider and transforms it into a list of atoms, one per ticker.
        '''
        raise NotImplementedError("_realtime_request is an abstract method, please implement it in a class")

    def stop(self):
        '''
        Stops the download of data.
        '''
        self.execute = False

    def _pre_process(self, atoms: Sequence[Mapping], **kwargs) -> Sequence[Mapping]:
        '''
        Optional metod to pre-process data before aliasing and date formatting.\n
        Atoms processing should be done here rather than in request because if it fails it won't try another attempt,
        because the error is not in the download but in the processing.\n

        Parameters:\n
            atoms : Sequence[Mapping]
                Atoms downloaded.\n
            kwargs
                Anything that the caller function can pass.\n
        '''
        return atoms

    def _post_process(self, atoms: Sequence[Mapping], **kwargs) -> Sequence[Mapping]:
        '''
        Optional method to further process atoms after all the standard processes like aliasing and date formatting.\n

        Parameters:\n
            atoms : Sequence[Mapping]
                Atoms downloaded and aliased.\n
            kwargs
                Anything that the caller function can pass.\n
        '''
        return atoms

    def _set_working_hours(self, start: time, stop: time):
        '''
        Defines when the algorithm should start and stop downloading (out of trading hours when there is no need).\n

        Parameters:\n
            start, stop : time
                Start and stop times of the day. Must contain timezone info.\n
        '''
        self.working_hours = {'start': start, 'stop': stop}


class MetadataDownloader(Downloader):
    '''
    Abstract class that defines a one-time download of metadata for tickers (could be fundamentals for the company
    or any useful piece of information).\n
    '''

    def __init__(self, provider_name: str, limiter:  RequestsLimiter, max_attempts: int = 2):
        '''
        Parameters:\n
            provider_name : str
                Name of the provider, will be used when storing data in the db.\n
            intervals : Intervals
                Defines supported intervals and their aliases for the request. It should extend the otri.downloader.Intervals class.\n
            limiter : RequestsLimiter
                A limiter object, should be shared with other downloaders too in order to work properly.\n
            max_attempts : int
                Maximum attempts to download historical data.\n
        '''
        super().__init__(provider_name=provider_name, limiter=limiter)
        self._set_max_attempts(max_attempts=max_attempts)
        self.datetime_formatter = lambda dt: th.datetime_to_str(th.str_to_datetime(dt))

    def info(self, ticker: str) -> Union[Sequence[Mapping], bool]:
        '''
        Retrieves information about the given tickers.\n

        Parameters:\n
            ticker : str
                Identifiers for financial objects.\n
        Returns:\n
            Info as a sequence of dicts if the request went well, False otherwise.\n
        '''
        # Attempt to download and parse data a number of times that is max_attempts
        attempts = 0
        while(attempts < self.max_attempts):
            try:
                # Check if there's any wait time to do
                wait_time = self.limiter.waiting_time()
                while wait_time > 0:
                    sleep(wait_time)
                    wait_time = self.limiter.waiting_time()

                # Request data as a list of atoms
                atom = self._info_request(ticker=ticker)
                break
            except Exception as err:
                attempts += 1
                log.w("error downloading {} on attempt {}: {}".format(ticker, attempts, err))
                # log.v(traceback.format_exc())
        else:
            # It reached the maximum number of attempts (while did not break)
            log.e("giving up download of {}, reached max attempts".format(ticker))
            return False

        # If no data is downloaded the ticker couldn't be found or there has been an error, we're not creating any output.
        if atom is None or not atom:
            log.w("empty downloaded data {}: {}".format(ticker, atom))
            return False

        # Optional atoms preprocessing
        preprocessed_atom = self._pre_process(atom=atom, tickers=ticker)
        # Process atoms keys using aliases and datetime formatter
        prepared_atom = {}
        # Renaming and filtering fields
        for key, value in self.aliases.items():
            if value is not None and value in preprocessed_atom:
                try:
                    prepared_atom[key] = preprocessed_atom[value]
                except Exception as e:
                    log.w("Exception thrown on renaming atom: {}. Exception: {}".format(preprocessed_atom, e))

        # Append provider
        prepared_atom['provider'] = [self.provider_name]

        # Further optional subclass processing
        postprocessed_atom = self._post_process(atom=prepared_atom, ticker=ticker)

        return postprocessed_atom

    def _info_request(self, ticker: str) -> Mapping:
        '''
        Method that requires data from the provider and transform it into a list of atoms.\n
        It should call the limiter._on_request and limiter._on_response methods if there is anything the limiter needs to know.\n
        Should NOT handle exceptions as they're catched in the superclass.\n

        Parameters:\n
            ticker : str
                Symbols to download metadata of.\n
        Returns:
            A single atom containing metadata.\n
        '''
        raise NotImplementedError("_info_request is an abstract method, please implement it in a class")

    def _pre_process(self, atom: Mapping, **kwargs) -> Mapping:
        '''
        Optional metod to pre-process data before aliasing and date formatting.\n
        Atoms processing should be done here rather than in request because if it fails it won't try another attempt,
        because the error is not in the download but in the processing.\n

        Parameters:\n
            atoms : Mapping
                Atom downloaded.\n
            kwargs
                Anything that the caller function can pass.\n
        '''
        return atom

    def _post_process(self, atom: Mapping, **kwargs) -> Mapping:
        '''
        Optional method to further process atoms after all the standard processes like aliasing and date formatting.\n

        Parameters:\n
            atom : Mapping
                Atom downloaded and aliased.\n
            kwargs
                Anything that the caller function can pass.\n
        '''
        return atom


class AdapterComponent(ABC):
    '''
    An adapter component is an object used by an adapter to perform standard operation between adapters of different sources.
    A component implements some of its interface's methods, the less the better.
    '''

    def prepare(self, **kwargs) -> Mapping:
        '''
        First of the download functionalities.
        '''
        return kwargs

    def retrieve(self, data_stream: WritableStream, **kwargs):
        '''
        Second of the download functionalities.

        Parameters:
            data_stream : WritableStream
                Stream where to place downloaded data do be parsed by an atomizer function.
        '''
        return kwargs

    def atomize(self, data_stream: ReadableStream, output_stream: WritableStream, **kwargs):
        '''
        Third of the download functionalities.

        Parameters:
            data_stream : ReadableStream
                Stream where downloaded data is placed to be parsed.
            output_stream : WritableStream
                Stream where to place parsed data to be read by others.
        '''
        return kwargs


class Adapter(ABC):
    '''
    Imports data from an external source.

    Attributes:
        components : list[AdapterComponent]
                Ordered list of adapter components that will be called on download. Default empty list.
         sync : bool
                Whether the adapter gets data synchronously (only once) or asynchronously (continuously, over time, multi-threaded).
                If the adapter is async the streams have to be closed by components.
    '''
    components : List[AdapterComponent]
    sync : bool = True


    def add_component(self, component: AdapterComponent):
        '''
        Appends a component at the end of the components list.

        Parameters:
            component : AdapterComponent
                Component to append.
        '''
        self.components.append(component)

    def download(self, o_stream: WritableStream = LocalStream(), **kwargs) -> LocalStream:
        '''
        Retrieves some data from a source.
        Each component is called in the passed order.

        Parameters:
            o_stream : WritableStream = LocalStream()
                Stream where to output parsed data. Should NOT be closed.

            Other parameters depend on what components the adapter uses.
        Returns:
            A closed stream of atoms, the same object as parameters o_stream.
        '''
        data_stream = LocalStream()
        for comp in self.components:
            kwargs = comp.prepare(**kwargs)

        # TODO: delet dis
        log.d("after prep: {}".format(kwargs))

        for comp in self.components:
            kwargs = comp.retrieve(data_stream=data_stream, **kwargs)
        if self._sync:
            data_stream.close()  # Close data stream after all downloads are performed, no more data can be added

        # TODO: delet dis
        log.d("after dw: {}".format(kwargs))

        for comp in self.components:
            kwargs = comp.atomize(data_stream=data_stream, output_stream=o_stream, **kwargs)
        if self._sync:
            o_stream.close()

        # TODO: delet dis
        log.d("after atomization: {}".format(kwargs))

        return o_stream


class TickerSplitterComp(AdapterComponent):
    '''
    Uses preparation phase to split a ticker list is multiple lists of a maximum size.
    Only uses prepare method.
    '''

    def __init__(self, max_count: int = 1, tickers_name : str = "tickers", ticker_groups_name : str = "ticker_groups"):
        '''
        Parameters:
            max_count : int
                Positive number for maximum number of tickers allowed per group/list/data request.
            tickers_name : str
                Name for ticker list parameter.
            ticker_groups_name : str
                Name for the output ticker groups.
        '''
        self._max_count = max_count
        self._tickers_name = tickers_name
        self._ticker_groups_name  = ticker_groups_name

    def prepare(self, **kwargs):
        # Checks
        if self._tickers_name not in kwargs:
            raise ValueError(f"Missing '{self._tickers_name}' parameter")
        if not isinstance(kwargs[self._tickers_name], Iterable):
            raise ValueError("'{self._tickers_name}' parameter is not iterable, it's {type(kwargs[self._tickers_name])}")

        kwargs[self._ticker_groups_name] = [kwargs[self._tickers_name][i:i + self._max_count]
                                   for i in range(0, len(kwargs[self._tickers_name]), self._max_count)]
        return kwargs

class ParamValidatorComp(AdapterComponent):
    '''
    Checks if a passed parameter is accepted.
    Only uses prepare method.
    '''

    def __init__(self, validator_mapping: Mapping):
        '''
        Parameters:
            validator_mapping : Mapping
                Dictionary where keys are parameter's names and values are validation methods taking one parameter.
                Validation methods should raise ValueError on failed validation.
                Validation methods should NOT modify the variables.
        '''
        self._validators = validator_mapping

    def prepare(self, **kwargs):
        for key, method in self._validators.items():
            method(kwargs.get(key, None))
        return kwargs

    # DEFAULT PARAMETER VALIDATION METHODS #
    @staticmethod
    def match_param_validation(key: str, possible_values: List, required: bool = True) -> Callable:
        '''
        Generates a validation method that checks if the parameter's value the possible values for the key.
        The method raises exception when the parameter's value is NOT between the possible ones.

        Parameters:
            key : str
                Only used in the raised exception when value is NOT in the possible ones.
            possible_values : list
                Collection of possible values for the parameter.
            required : bool
                Whether the parameter is required or not.
        Returns:
            A validation method.
        '''
        def validator(value):
            if value is None and required:
                raise ValueError("Parameter '{}' cannot be None".format(key))
            if not required:
                possible_values.append(None)
            if value not in possible_values:
                raise ValueError("{} not a possible value for '{}', possible values: {}".format(
                    value, key, possible_values))
        return validator
    
    @staticmethod
    def datetime_param_validation(key : str, dt_format : str, required = True) -> Callable:
        '''
        Generates a validation method that checks if the parameter's value is a datetime with the given format.
        The method raises exception when the parameter's value is NOT a datetime or in the given format.

        Parameters:
            key : str
                Only used in the raised exception when value is wrong.
            dt_format : str
                strptime format to parse the parameter's value.
            required : bool
                Whether the parameter is required or not.
        '''
        def validator(value):
            if value is None and required:
                raise ValueError("Parameter '{}' cannot be None".format(key))
            if not isinstance(value, str):
                raise ValueError("Parameter '{}' is not a string".format(key))
            try:
                datetime.strptime(value, dt_format)
            except ValueError:
                raise ValueError("Parameter '{}' with value {} does not match datetime format {}".format(key, value, dt_format))
        return validator

    # TODO: another default validation could be range check (value in range [min, max])
    # TODO: another default validation is just checking that a parameter is given

# TODO: preparation component that translates/maps values (eg. request for "history" becomes the url to require "market/history")

class MappingComp(AdapterComponent):
    '''
    Changes parameter's values according to a given mapping.
    Can be used to reuse the same adapter for different downloads
    '''

    def __init__(self, key : str, value_mapping : Mapping[str, Set[str]], required : bool = True):
        '''
        Parameters:
            key : str
                Key where to change values.
            value_mapping : Mapping[str, Set[str]]
                Mapping where key destination translation and value is a list/set of values to translate into the key.
                eg. {'A': {'B', 'C'}}
                'B' -> 'A'
                'C' -> 'A'
            required : bool
                Whether the given mapping 
        '''
        self._key = key
        for value in value_mapping.values():
            if not isinstance(value, Iterable):
                raise TypeError("Mapping's value for key {} is not an iterable, {} found".format(key, type(value)))
        self._value_mapping = value_mapping
        self._required = required

    def prepare(self, **kwargs) -> Mapping:
        if self._key not in kwargs and self._required:
            raise ValueError("Parameter '{}' cannot be None".format(self._key))
        elif self._key in kwargs:
            # Compute possible values
            possible_values = list()
            for values in self._value_mapping.values():
                possible_values.extend(values)
            # Check if there is defined a translation for the value
            if kwargs[self._key] not in possible_values:
                raise ValueError("Parameter's '{}' value {} is not in {}".format(
                    self._key,
                    kwargs[self._key],
                    possible_values
                ))
            # Search for value and replace it with the first occurrency
            for key, values in self._value_mapping.items():
                if kwargs[self._key] in values:
                    kwargs[self._key] = key
                    break

        return kwargs

class TickerGroupHandler(AdapterComponent):
    '''
    Aggregator component that handles multiple ticker groups by performing the prepration and download phase for each of the groups.
    '''

    def __init__(self, components : List[AdapterComponent], ticker_groups_name : str = 'ticker_groups', tickers_name : str = 'tickers'):
        '''
        Parameters:
            components : list[AdapterComponent]
                Ordered list of adapter components that will be called for prepariation and retrieve phase.
            ticker_groups_name : str
                Name of the parameter containing ticker groups (a list of lists of strings).
            tickers_name : str
                Name for tickers parameter to pass to sub-components.
        '''
        self._components = components
        self._ticker_groups_name = ticker_groups_name
        self._tickers_name = tickers_name

    def retrieve(self, data_stream, **kwargs):
        # Checks
        if self._ticker_groups_name not in kwargs:
            raise ValueError("Missing tickers group parameter '{}'".format(self._ticker_groups_name))
        if not isinstance(kwargs[self._ticker_groups_name], Iterable):
            raise ValueError("Parameter '{}' should be of type Iterable, {} found".format(self._ticker_groups_name, type(kwargs[self._ticker_groups_name])))
        
        for group in kwargs[self._ticker_groups_name]:
            # TODO: check group is still an iterable
            kwargs_copy = kwargs.copy()
            kwargs_copy[self._tickers_name] = group
            # Prepare
            for comp in self._components:
                kwargs_copy = comp.prepare(**kwargs_copy)
            # Retrieve
            for comp in self._components:
                kwargs_copy = comp.retrieve(data_stream=data_stream, **kwargs_copy)
            kwargs.update(kwargs_copy)

        return kwargs


class RequestComp(AdapterComponent):
    '''
    Performs and HTTP request to an url with given parameters.
    Only uses retrieve method.
    Response data is passed as text or as json to the output data_stream.
    '''

    def __init__(self, base_url: str, url_key: str, query_param_names: Set = set(),
                 header_param_names: Set = set(), default_query_params: Mapping = dict(),
                 default_header_params: Mapping = dict(), timeout: float = 10, to_json: bool = False):
        '''
        Parameters:
            base_url : str
                Base url for HTTP request, should contain at the beginning 'http://' or 'https://'.
            url_key : str
                What key of the adapter parameters contains the specific URL
            query_param_names : Set
                Collection of keys that are included in the HTTP request url (query parameters). The values are read from kwargs.
            header_param_names : Set
                Collection of keys that are included in the HTTP request header. The values are read from kwargs.
            default_get_params : Mapping
                Dictionary of default values for query parameters. Keys can overlap with query_param_names.
            default_header_params : Mapping
                Dictionary of default values for header parameters. Keys can overlap with header_param_names.
            timeout : float
                Maximum wait time for request response. Default 10s.
            to_json : bool
                Whether parse response as json or leave it as text. Default False.
        '''
        self._base_url = base_url
        self._url_key = url_key
        self._query_param_names = query_param_names
        self._header_param_names = header_param_names
        self._query_params = default_query_params
        self._header_params = default_header_params
        self._timeout = timeout
        self._to_json = to_json

    def retrieve(self, data_stream: WritableStream, **kwargs):
        # Check if url key and value is present
        if self._url_key not in kwargs:
            raise ValueError("Missing '{}' parameter that defines the HTTP request url".format(self._url_key))

        log.d(f"kwargs: {kwargs}")

        # Create query parameter dictionary
        for key in self._query_param_names:
            if key in kwargs:
                self._query_params[key] = kwargs[key]
        # Create header parameter dictionary
        for key in self._header_param_names:
            if key in kwargs:
                self._header_params[key] = kwargs[key]


        log.d(f"query parameters: {self._query_params}")
        log.d(f"header parameters: {self._header_params}")

        # Perform HTTP request
        response = requests.get(self._base_url + kwargs[self._url_key],
                                params=self._query_params,
                                headers=self._header_params,
                                timeout=self._timeout,
                                )
        # Check if the request went well
        if response is None or response is False:
            raise ValueError(f"Empty HTTP response (url: {self._base_url + kwargs[self._url_key]}, params: {self._query_params}, headers: {self._header_params})")
        if response.status_code < 200 and response.status_code >= 300:
            raise ValueError(f"HTTP status code not 200 (code: {response.status_code}, response: {response.text})")

        # Output the response
        data = response.text
        if self._to_json:
            data = response.json()
        data_stream.append(data)

        log.d(data)

        # Set in kwargs some maybe useful data, the response headers
        kwargs['_last_headers'] = response.headers
        return kwargs

class TickerExtractorComp(AdapterComponent):
    '''
    Converts a list of a single ticker into a single ticker parameter.
    '''
    def __init__(self, ticker_coll_name : str = 'tickers', ticker_name : str = 'ticker'):
        '''
        Parameters:
            ticker_coll_name : str
                Name of the ticker collection. Default 'tickers'.
            ticker_name : str
                Output name for the single ticker parameter. Default 'ticker'.
        '''
        self._ticker_coll_name = ticker_coll_name
        self._ticker_name = ticker_name

    def prepare(self, **kwargs):
        if not isinstance(kwargs[self._ticker_coll_name], Iterable):
            raise ValueError(f"'{self._ticker_coll_name}' parameter is not of type Iterable, {kwargs[self._ticker_coll_name]} found")
        if len(kwargs[self._ticker_coll_name]) <= 0:
            raise ValueError(f"'{self._ticker_coll_name}' parameter cannot be empty")
        if len(kwargs[self._ticker_coll_name]) > 1:
            raise ValueError(f"'{self._ticker_coll_name}' parameter must have only 1 element, {len(kwargs[self._ticker_coll_name])} found")

        kwargs[self._ticker_name] = kwargs[self._ticker_coll_name][0]
        return kwargs
