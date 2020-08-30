
__author__ = "Luca Crema <lc.crema@hotmail.com>, Riccardo De Zen <riccardodezen98@gmail.com>"
__version__ = "2.0"

from datetime import date, timedelta, datetime
from queue import Queue
from typing import Any, Mapping, Sequence, Union, Callable
from ..utils import logger as log, key_handler as kh, time_handler as th
from time import sleep

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


class DownloadLimiter:
    '''
    Object that handles the provider requests limitations.
    Could be as simple as the DefaultDownloadLimiter or something more complex that uses something in the request or response.\n
    Must be thread safe.
    '''

    def waiting_time(self):
        '''
        Calculates the amount of time the downloader should wait in order not to exceed provider limitations.\n
        Returns:\n
            The amount of sleep time in seconds. 0 if no sleep time is needed.
        '''
        raise NotImplementedError("This is an abstract method, please implement it in a class")

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


class DefaultDownloadLimiter(DownloadLimiter):
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
        log.i("c:{} max:{}".format(self.request_counter, self.max_requests))
        if(self.request_counter < self.max_requests):
            return 0
        return (self.next_reset - datetime.utcnow()).total_seconds()


class Downloader:
    '''
    Defines an interface with a data provider of any kind.
    '''

    aliases = {
        'datetime': None
    }

    # Default class limiter, can be used to avoid keeping track of provider specific parameters.
    DEFAULT_LIMITER = DownloadLimiter()

    def __init__(self, provider_name: str):
        '''
        Parameters:\n
            provider_name : str
                Name of the provider, will be used when storing data in the db.\n
        '''
        self.provider_name = provider_name

    def _set_aliases(self, aliases: Mapping[str, str]):
        '''
        Extends the current aliases dictionary with new aliases overriding current ones.\n

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

    def _set_limiter(self, limiter: DownloadLimiter):
        '''
        Parameters:\n
            limiter : DownloadLimiter
                Limiter object that handles the provider request limitations.\n
        '''
        self.limiter = limiter


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

    def __init__(self, provider_name: str, intervals: Intervals):
        '''
        Parameters:\n
            provider_name : str
                Name of the provider, will be used when storing data in the db.\n
            intervals : Intervals
                Defines supported intervals and their aliases for the request. It should extend the otri.downloader.Intervals class.\n
        '''
        super().__init__(provider_name=provider_name)
        self.intervals = intervals
        self.request_dateformat = "%Y-%m-%d %H:%M"
        self.datetime_formatter = lambda datetime: th.datetime_to_str(th.str_to_datetime(datetime))

    def history(self, ticker: str, start: datetime, end: datetime, interval: Intervals) -> Union[dict, bool]:
        '''
        Downloads time-series data for a single ticker given two dates.\n

       Parameters:\n
            ticker : str
                The simbol to download data of.\n
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
            raise Exception("Interval not supported by {}".format(self.provider_name))

        data = dict()
        # Attempt to download and parse data a number of times that is max_attempts
        attempts = 0
        while(attempts < self.max_attempts):
            try:
                # Check if there's any wait time to do
                wait_time = self.limiter.waiting_time()
                if wait_time > 0:
                    log.w("exceeded download rates, waiting {} seconds before performing request".format(wait_time))
                    sleep(wait_time)
                # Request data as a list of atoms
                atom_list = self._request(ticker, start.strftime(self.request_dateformat),
                                          end.strftime(self.request_dateformat), interval)
                break
            except Exception as err:
                attempts += 1
                log.w("{} - error downloading {} on attempt {}: {}".format(self.provider_name, ticker, attempts, err))

        # Chech if it reached the maximum number of attempts
        if(attempts >= self.max_attempts):
            log.e("giving up download of {}, reached max attempts".format(ticker))
            return False

        # If no data is downloaded the ticker couldn't be found or there has been an error, we're not creating any output.
        if atom_list is None or not atom_list:
            log.w("empty downloaded data {}: {}".format(ticker, atom_list))
            return False

        # Optional atoms preprocessing
        preprocessed_atoms = self._pre_process(atom_list)
        # Process atoms keys using aliases and datetime formatter
        prepared_atoms = []
        for atom in preprocessed_atoms:
            new_atom = {}
            # Renaming
            for key, value in self.aliases.items():
                if value is not None:
                    new_atom[key] = atom[value]
            # Datetime formatting
            try:
                new_atom['datetime'] = self.datetime_formatter(new_atom['datetime'])
            except KeyError:
                log.w("missing atoms datetime: {}".format(new_atom))
            prepared_atoms.append(new_atom)

        # Further optional subclass processing
        postprocessed_atoms = self._post_process(prepared_atoms)
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
                String format passed to datetime.strptime before giving the date to the request method.\n
        '''
        self.request_dateformat = request_dateformat

    def _set_datetime_formatter(self, formatter: Callable):
        '''
        Sets a different datetime formatter for atoms than the default one.

        Parameters:
            datetime_formatter : str
                Method that takes a datetime string as parameter and returns a properly formatted YYYY-MM-DD HH:mm:ss.fff datetime string.\n
        '''
        self.datetime_formatter = formatter

    def _request(self, ticker: str, start: date, end: date, interval: str) -> list:
        '''
        Method that requires data from the provider and transform it into a list of atoms.\n
        It should call the limiter._on_request and limiter._on_response methods if there is anything the limiter needs to know.\n
        Should NOT handle exceptions as they're catched in the superclass.\n

        Parameters:\n
            ticker : str
                The simbol to download data of.\n
            start : date
                Must be before end.\n
            end : date
                Must be after and different from start.\n
            interval : str
                Its possible values depend on the intervals attribute.
        '''
        raise NotImplementedError("This is an abstract method, please implement it in a class")

    def _pre_process(self, atoms: Sequence[Mapping]) -> Sequence[Mapping]:
        '''
        Optional metod to pre-process data before aliasing and date formatting.\n
        Atoms processing should be done here rather than in request because if it fails it won't try another attempt,
         because the error is not in the download but in the processing.\n

        Parameters:\n
            atoms : Sequence[Mapping]
                atoms downloaded and alised.\n
        '''

    def _post_process(self, atoms: Sequence[Mapping]) -> Sequence[Mapping]:
        '''
        Optional method to further process atoms after all the earlier processes like aliasing and date formatting.\n

        Parameters:\n
            atoms : Sequence[Mapping]
                atoms downloaded and alised.\n
        '''
        return atoms


class OptionsDownloader(Downloader):
    '''
    Abstract class that defines downloading of options chain, "contracts" history, bids and asks.\n
    The download should be performed only once and not continuosly.
    '''

    def expirations(self, ticker: str) -> Union[Sequence[str], bool]:
        '''
        Retrieves the list of expiration dates for option contracts.\n

        Parameters:\n
            ticker : str
                Name of the symbol to get the list of.\n

        Returns:\n
            An ordered sequence of dates as strings of option expiration dates.
        '''
        raise NotImplementedError("This is an abstract method, please implement it in a class")

    def history(self, contract: str, start: date, end: date, interval: str = "1m") -> Union[dict, bool]:
        '''
        Retrieves a timeseries-like history of a contract.\n

        Parameters:\n
            contract : str
                Name of the contract, usually in the form "ticker"+"date"+"C for calls or P for puts"+"strike price"\n
            start : date
                Must be before end.\n
            end : date
                Must be after and different from start.\n
            interval : str
                Frequency for data.\n

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
                - volume
        '''
        raise NotImplementedError("This is an abstract method, please implement it in a class")

    def chain(self, ticker: str, expiration: str, kind: str) -> Union[dict, bool]:
        '''
        Retrieves the list of call contracts for the given ticker and expiration date.\n

        Parameters:\n
            ticker : str
                Name of the symbol.\n
            expiration : str
                Expiration date as string, must have been obtained using the get_expiration method.\n
            kind : str
                "calls" or "puts"\n

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
        raise NotImplementedError("This is an abstract method, please implement it in a class")

    def chain_contracts(self, ticker: str, expiration: str, kind: str) -> Sequence[str]:
        '''
        Retrives a sequence of contract name/ticker for the given ticker, expiration and type (call or put).\n

        Parameters:\n
            ticker : str
                Name of the symbol.\n
            expiration : str
                Expiration date, must have been obtained using the get_expiration method.\n
            kind : str
                "calls" or "puts"\n

        Returns:\n
            A sequence of contract symbol names (tickers) ordered by the most in the money to the most out of the money.
        '''
        raise NotImplementedError("This is an abstract method, please implement it in a class")


class RealtimeDownloader(Downloader):
    '''
    Abstract class that defines a continuous download of data by sending multiple requests to the provider.\n
    For streaming see StreamingDownloader (Not implemented yet).\n
    '''

    def start(self, tickers: Union[str, Sequence[str]], delay: float, contents_queue: Queue):
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
        raise NotImplementedError("This is an abstract method, please implement it in a class")

    def stop(self):
        '''
        Stops the download of data.
        '''
        raise NotImplementedError("This is an abstract method, please implement it in a class")
