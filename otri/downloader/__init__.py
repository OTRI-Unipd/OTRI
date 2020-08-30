
__author__ = "Luca Crema <lc.crema@hotmail.com>, Riccardo De Zen <riccardodezen98@gmail.com>"
__version__ = "1.2"

from datetime import date, timedelta, datetime
from queue import Queue
from typing import Any, Mapping, Sequence, Union
from ..utils import time_handler as th

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

    def __init__(self, requests_number: int, timespan: timedelta):
        '''
        Parameters:\n
            requests_number : int
                Number of requests that can be made per timespan.\n
            timespan : timedelta
                Amount of time where the limit is defined.\n
        '''
        self.max_requests = requests_number
        self.timespan = timespan
        self.next_reset = datetime(2000, 1, 1)
        self.request_counter = 0

    def _on_request(self, request_data: Any = None):
        '''
        Called when performing a request. Updates the requests number.
        '''
        # If enough time has passed we can reset the counter.
        if(datetime.now() > self.next_reset):
            self.next_reset = datetime.now() + self.timespan
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
        return (self.next_reset - datetime.utcnow()).total_seconds()


class Downloader:
    '''
    Defines an interface with a data provider of any kind.
    '''

    def __init__(self, provider_name):
        '''
        Parameters:\n
            provider_name : str
                Name of the provider, will be used when storing data in the db.\n
        '''
        self.provider_name = provider_name

    def _set_aliases(self, aliases: Mapping[str, str]):
        '''
        Parameters:\n
            aliases : Mapping[str, str]
                Key-value pairs that define the renaming of atoms' keys. Values must be all lowecased.
        '''
        self.aliases = aliases

    def _set_max_attempts(self, max_attempts: int):
        '''
        Parameters:
            max_attempts : int
                Number of maximum attempts the downloader will do to download data. Does not include the data elaboration,
                if something goes wrong when working on downloaded data the script won't attempt to download it again.
        '''
        self.max_attempts = max_attempts

    def _set_limiter(self, limiter: DownloadLimiter):
        '''
        Parameters:\n
            limiter : DownloadLimiter
                Limiter object that handles the provider request limitations.
        '''
        self.limiter = limiter


class TimeseriesDownloader(Downloader):
    '''
    Defines historical time series data downloading.\n
    The download should be performed only once and not continuosly.
    '''

    def history(self, ticker: str, start: date, end: date, interval: str) -> Union[dict, bool]:
        '''
        Downloads time-series data for a single ticker given two dates.\n

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
                - high\n
                - low\n
                - close\n
                - volume\n
        '''
        raise NotImplementedError(
            "This is an abstract method, please implement it in a class")


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
