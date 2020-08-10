'''
Console module to download and upload timeseries stock data.

Usage:
python timeseries_cli_dw.py -p [PROVIDER] -f [TICKERS_FILE] -t [THREAD COUNT]
'''

__autor__ = "Luca Crema <lc.crema@hotmail.com>"
__version__ = "0.1"

import json
import math
import threading
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List

import otri.utils.config as config
import otri.utils.logger as log
from otri.database.postgresql_adapter import PostgreSQLAdapter
from otri.downloader.alphavantage_downloader import AVTimeseriesDW
from otri.downloader.timeseries_downloader import TimeseriesDownloader
from otri.downloader.yahoo_downloader import YahooTimeseriesDW
from otri.importer.data_importer import DataImporter, DefaultDataImporter
from otri.utils.cli import CLI, CLIValueOpt

DATA_FOLDER = Path("data/")
# downloader : (obj, download delay)
DOWNLOADERS = {
    "YahooFinance": (YahooTimeseriesDW(), 0),
    "AlphaVantage":  (AVTimeseriesDW(config.get_value("alphavantage_api_key")), 15)
}
TICKER_LISTS_FOLDER = Path("docs/")


class DownloadJob(threading.Thread):
    def __init__(self, tickers: List[str],downloader : TimeseriesDownloader, timeout_time : float, importer : DataImporter):
        super().__init__()
        self.shutdown_flag = threading.Event()
        self.tickers = tickers
        self.downloader = downloader
        self.importer = importer
        self.timeout_time = timeout_time

    def run(self):
        for ticker in self.tickers:
            # Actually download data
            downloaded_data = self.downloader.download_between_dates(
                ticker=ticker, start=start_date, end=end_date, interval="1m")
            if(downloaded_data == False):
                log.e("unable to download {}".format(ticker))
                time.sleep(self.timeout_time)
                continue
            # Upload data
            log.i("attempting to upload {}".format(ticker))
            self.importer.from_contents(downloaded_data)
            log.i("successfully uploaded {}".format(ticker))
            # Could refactor to wait timeout time - upload time
            time.sleep(self.timeout_time)


def service_shutdown(signum, frame):
    print('Caught signal %d' % signum)
    raise ServiceExit


class ServiceExit(Exception):
    """
    Custom exception which is used to trigger the clean exit
    of all running threads and the main program.
    """
    pass


def list_tickers_file(ticker_list_folder: Path) -> Path:
    '''
    List json files from the docs folder where to find the ticker list.

    Returns:
        Path to the selected ticker list file.
    '''
    docs_glob = ticker_list_folder.glob('*.json')
    return [x.name.replace('.json', '') for x in docs_glob if x.is_file()]


def retrieve_ticker_list(doc_path: Path) -> List[str]:
    '''
    Grabs all tickers from the properly formatted doc_path file.

    Returns:
        A list of str, names of tickers.
    '''
    doc = json.load(doc_path.open("r"))
    return [ticker['ticker'] for ticker in doc['tickers']]


def print_error_msg(msg: str = None):
    if msg is None:
        log.e("timeseries_cli_download.py -p <provider: {}> -f <ticker file: {}>".format(
            list(DOWNLOADERS.keys()), list_tickers_file(TICKER_LISTS_FOLDER)
        )
        )
    else:
        log.e("{}: timeseries_cli_download.py -p <provider: {}> -f <ticker file: {}>".format(
            msg,
            list(DOWNLOADERS.keys()),
            list_tickers_file(TICKER_LISTS_FOLDER)
        )
        )


def get_seven_days_ago() -> date:
    '''
    Calculates when is 7 days ago.

    Returns:
        The date of 7 days ago.
    '''
    seven_days_delta = timedelta(days=7)
    seven_days_ago = datetime.now() - seven_days_delta
    return date(seven_days_ago.year, seven_days_ago.month, seven_days_ago.day)


if __name__ == "__main__":

    cli = CLI(name = "timeseries_cli_dw",
    description = "Script that downloads weekly historical timeseries data.",
    options=[
        CLIValueOpt(
            short_name="p",
            long_name="provider",
            short_desc="Provider",
            long_desc="Provider for the historical data.",
            required=True,
            values=list(DOWNLOADERS.keys())
        ),
        CLIValueOpt(
            short_name="f",
            long_name="file",
            short_desc="Ticker file",
            long_desc="File containing tickers to download.",
            required=True,
            values=list_tickers_file(TICKER_LISTS_FOLDER)
        ),
        CLIValueOpt(
            short_name="t",
            long_name="threads",
            short_desc="Threads",
            long_desc="Number of threads where tickers will be downloaded in parallel.",
            required=False,
            default="1"
        )
    ])

    values = cli.parse()
    provider = values["-p"]
    ticker_file = values["-f"]
    thread_count = int(values["-t"])

    if thread_count < 0:
        thread_count = 1

    # Retrieve the ticker list from the chosen file
    tickers = retrieve_ticker_list(Path(TICKER_LISTS_FOLDER, "{}.json".format(ticker_file)))
    start_date = get_seven_days_ago()
    end_date = date.today()

    # Setup downloader and timeout time
    downloader = DOWNLOADERS[provider][0]
    timeout_time = DOWNLOADERS[provider][1]

    # Setup database connection
    database_adapter = PostgreSQLAdapter(
        config.get_value("postgre_username"),
        config.get_value("postgre_password"),
        config.get_value("postgre_host"))
    importer = DefaultDataImporter(database_adapter)

    log.i("beginning download from provider {} of tickers {} from {} to {}".format(
        provider, ticker_file, start_date, end_date))

    # Multithreading
    threads = []

    # Split ticker in threads_count groups
    if(thread_count <= 0):
        thread_count = math.sqrt(len(tickers))
    n = round(len(tickers)/thread_count)
    ticker_groups = [tickers[i:i + n] for i in range(0, len(tickers), n)]
    log.i("splitting in {} threads with {} tickers each".format(len(ticker_groups), n))

    for t_group in ticker_groups:
        t = DownloadJob(t_group, downloader, timeout_time, importer)
        threads.append(t)
        t.start()

    # Waiting for threads to finish
    for thread in threads:
        thread.join()

    log.i("download completed")
