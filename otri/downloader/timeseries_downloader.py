from datetime import date
from typing import Union

ATOMS_KEY = "atoms"
METADATA_KEY = "metadata"
META_TICKER_KEY = "ticker"
META_INTERVAL_KEY = "interval"
META_PROVIDER_KEY = "provider"


class TimeseriesDownloader:
    '''
    Abstract class that defines any type of data downloading from any source of time series.
    '''

    def download_between_dates(self, ticker: str, start: date, end: date, interval: str) -> Union[dict, bool]:
        '''
        Downloads quote data for a single ticker given two dates.\n

       Parameters:\n
            ticker : str\n
                The simbol to download data of.\n
            start : date\n
                Must be before end.\n
            end : date\n
                Must be after and different from start.\n
            interval : str\n
                Could be "1m" (7 days max); "2m", "5m", "15m", "30m", "90m" (60 days max); "60m", "1h" (730 days max); "1d", "5d", "1wk"\n
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
            "This is an abstract method, please implement it in a child class")
