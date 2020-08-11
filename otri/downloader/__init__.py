
__author__ = "Luca Crema <lc.crema@hotmail.com>, Riccardo De Zen <riccardodezen98@gmail.com>"
__version__ = "1.2"

from datetime import date
from typing import Union, Sequence
from queue import Queue

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


class TimeseriesDownloader:
    '''
    Abstract class that defines historical time series data downloading.\n
    The download should be performed only once and not continuosly.
    '''

    def history(self, ticker: str, start: date, end: date, interval: str) -> Union[dict, bool]:
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
        raise NotImplementedError(
            "This is an abstract method, please implement it in a class")


class OptionsDownloader:
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


class RealtimeDownloader:
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
