from datetime import date
from typing import Union, Sequence

ATOMS_KEY = "atoms"
METADATA_KEY = "metadata"
META_TICKER_KEY = "ticker"
META_TYPE_KEY = "option type"
META_PROVIDER_KEY = "provider"
META_DOWNLOAD_TIME = "download datetime"
META_EXPIRATION_DATE = "expiration"
META_OPTION_TYPE_KEY = "option type"
META_TYPE_KEY = "type"
META_TYPE_VALUE = "option"


class OptionsDownloader:
    '''
    Abstract class that defines downloading of options chain, "contracts" history, bids and asks.
    '''

    def get_expirations(self, ticker: str) -> Sequence[str]:
        '''
        Retrieves the list of expiration dates for option contracts.\n

        Parameters:\n
            ticker : str\n
                Name of the symbol to get the list of.\n

        Returns:\n
            An ordered sequence of dates as strings of option expiration dates.\n
        '''
        return NotImplementedError()

    def get_history(self, contract: str, start: date, end: date, interval: str = "1m") -> Union[dict, bool]:
        '''
        Retrieves a timeseries-like history of a contract.\n

        Parameters:\n
            contract : str\n
                Name of the contract, usually in the form "ticker"+"date"+"C for calls or P for puts"+"strike price"\n
            start : date\n
                Must be before end.\n
            end : date\n
                Must be after and different from start.\n
            interval : str\n
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
                - volume\n
        '''
        return NotImplementedError()

    def get_chain(self, ticker: str, expiration: str, kind : str) -> Union[dict, bool]:
        '''
        Retrieves the list of call contracts for the given ticker and expiration date.\n

        Parameters:\n
            ticker : str\n
                Name of the symbol.\n
            expiration : str\n
                Expiration date, must have been obtained using the get_expiration method.\n
            kind : str\n
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
                - in the money (true or false)\n
        '''
        return NotImplementedError()

    def get_chain_contracts(self, ticker : str, expiration: str, kind : str) -> Sequence[str]:
        '''
        Retrives a sequence of contract name/ticker for the given ticker, expiration and type (call or put).\n

        Parameters:\n
            ticker : str\n
                Name of the symbol.\n
            expiration : str\n
                Expiration date, must have been obtained using the get_expiration method.\n
            kind : str\n
                "calls" or "puts"\n

        Returns:\n
            A sequence of contract symbol names (tickers) ordered by the most in the money to the most out of the money.\n
        '''
        return NotImplementedError()