from alpha_vantage.timeseries import TimeSeries
from .timeseries_downloader import TimeseriesDownloader, Union, METADATA_KEY, META_INTERVAL_KEY, META_PROVIDER_KEY, META_TICKER_KEY, ATOMS_KEY
from datetime import date, datetime
from pytz import timezone
from ..utils import key_handler as key_handler
import json

GMT = timezone("GMT")
TIME_ZONE_KEY = "6. Time Zone"
AV_ALIASES = {
    "1. open": "open",
    "2. high": "high",
    "3. low": "low",
    "4. close": "close",
    "5. volume": "volume"
}
META_PROVIDER_VALUE = "alpha vantage"


class AVDownloader(TimeseriesDownloader):
    '''
     Used to download Timeseries data from AlphaVantage.
    '''

    def __init__(self, api_key: str):
        '''
        Init method.
        Parameters:
            key : str
                the Alpha Vantage API key to use
        '''
        self.ts = TimeSeries(api_key, output_format='pandas')

    def download_between_dates(self, ticker: str, start: date, end: date, interval: str = "1m", debug: bool = False) -> Union[dict, bool]:
        '''
        Downloads quote data for a single ticker given the start date and end date.

        Parameters:
            ticker : str
                The simbol to download data of.
            start_datetime : datetime
                Must be before end_datetime.
            end_datetime : datetime
                Must be after and different from start_datetime.
            interval : str
                Could be "1m", "5m", "15m", "30m", "60m" (for intraday) "1d" (for daily) "1wk" (for weekly)
        Returns:
            False if there has been an error,
            a dict containing "metadata" and "atoms" otherwise.

            metadata is a dict containing at least:
                - ticker
                - interval
                - provider
                - other data that the atomizer could want to apply to every atom

            atoms is a list of dicts containing:
                - datetime (format Y-m-d H:m:s.ms)
                - other financial values
        '''
        av_interval = AVDownloader.__standardize_interval(interval)
        try:
            values, meta = self.__call_timeseries_function(
                ticker=ticker, interval=av_interval, start_date=start)
        except ValueError as exception:
            if debug:
                print("AlphaVantage ValueError: ", exception)
            return False
        dict_data = json.loads(values.to_json(orient="table"))
        atoms = dict_data['data']
        atoms = AVDownloader.__fix_atoms_datetime(
            atoms=atoms, tz=meta[TIME_ZONE_KEY])
        atoms = key_handler.rename_deep(atoms, AV_ALIASES)
        atoms = AVDownloader.__filter_atoms_by_date(
            atoms=atoms, start_date=start, end_date=end)
        data = dict()
        data[ATOMS_KEY] = atoms
        data[METADATA_KEY] = {META_TICKER_KEY: ticker,
                            META_INTERVAL_KEY: interval,
                            META_PROVIDER_KEY: META_PROVIDER_VALUE,
                            "last refreshed": meta['3. Last Refreshed']}
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
            return self.ts.get_weekly_adjusted(symbol=ticker)
        if(interval == "1d"):
            return self.ts.get_daily_adjusted(symbol=ticker, outputsize='full')
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
            start_date.year, start_date.month, start_date.day)
        end_datetime = datetime(end_date.year, end_date.month, end_date.day)

        for atom in atoms:
            atom_datetime = datetime.strptime(
                atom['datetime'], "%Y-%m-%d %H:%M:%S.%f")
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
            atom["datetime"] = AVDownloader.__convert_to_gmt(date_time=datetime.strptime(atom.pop("date"), "%Y-%m-%dT%H:%M:%S.%fZ"),
                                                             zonename=tz).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        return atoms

    @staticmethod
    def __convert_to_gmt(*, date_time: datetime, zonename: str) -> datetime:
        '''
        Method to convert a datetime in a certain timezone to a GMT datetime.
        Parameters:
            date_time : datetime
                The datetime to convert.
            zonename : str
                The time zone's name.
        Returns:
            The datetime object in GMT time.
        '''
        zone = timezone(zonename)
        base = zone.localize(date_time)
        return base.astimezone(GMT)

    @staticmethod
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
