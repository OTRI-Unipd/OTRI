'''
Console module to download and upload timeseries stock data.
'''

__autor__ = "Luca Crema <lc.crema@hotmail.com>"
__version__ = "0.1"

import getopt
import json
import sys
import time
import threading
import signal
import math
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List

import otri.utils.config as config
import otri.utils.logger as log
from otri.database.postgresql_adapter import DatabaseQuery, PostgreSQLAdapter
from otri.downloader.alphavantage_downloader import AVDownloader
from otri.downloader.timeseries_downloader import TimeseriesDownloader
from otri.downloader.yahoo_downloader import YahooDownloader
from otri.importer.data_importer import DataImporter, DefaultDataImporter

DATA_FOLDER = Path("data/")
# downloader : (obj, download delay)
DOWNLOADERS = {
    "YahooFinance": (YahooDownloader(), 0),
    "AlphaVantage":  (AVDownloader(config.get_value("alphavantage_api_key")), 15)
}

TICKER_LISTS_FOLDER = Path("docs/")


class DownloadJob(threading.Thread):
    def __init__(self, tickers: List[str],downloader : TimeseriesDownloader, importer : DataImporter):
        super().__init__()
        self.shutdown_flag = threading.Event()
        self.tickers = tickers
        self.downloader = downloader
        self.importer = importer

    def run(self):
        for ticker in self.tickers:
            # Actually download data
            downloaded_data = self.downloader.download_between_dates(
                ticker=ticker, start=start_date, end=end_date, interval="1m")
            if(downloaded_data == False):
                log.e("Unable to download {}".format(ticker))
                continue
            # Upload data
            log.i("attempting to upload {}".format(ticker))
            self.importer.from_contents(downloaded_data)
            log.i("successfully uploaded {}".format(ticker))
            time.sleep(timeout_time)


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
        print("timeseries_cli_download.py -p <provider: {}> -f <ticker file: {}>".format(
            list(DOWNLOADERS.keys()), list_tickers_file(TICKER_LISTS_FOLDER)
        )
        )
    else:
        print("{}: timeseries_cli_download.py -p <provider: {}> -f <ticker file: {}>".format(
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

    if len(sys.argv) < 2:
        print_error_msg("Not enough arguments")
        sys.exit(2)

    provider = ""
    ticker_file = ""
    thread_count = 0
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hp:f:t:", ["help", "provider=", "file=","threads="])
    except getopt.GetoptError as e:
        # If the passed option is not in the list it throws error
        print_error_msg(e)
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print_error_msg()
            sys.exit()
        elif opt in ("-p", "--provider"):
            provider = arg
        elif opt in ("-f", "--file"):
            ticker_file = arg
        elif opt in ("-t", "--threads"):
            thread_count = int(arg)

    if provider == "" or ticker_file == "":
        print_error_msg("Not enough arguments")
        sys.exit(2)

    if not provider in list(DOWNLOADERS.keys()):
        print_error_msg("Provider {} not supported".format(provider))
        sys.exit(2)

    if not ticker_file in list_tickers_file(TICKER_LISTS_FOLDER):
        print_error_msg("Ticker file {} not supported".format(ticker_file))
        sys.exit(2)

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
        t = DownloadJob(t_group, downloader, importer)
        threads.append(t)
        t.start()

    # Waiting for threads to finish
    for thread in threads:
        thread.join()

    log.i("download completed")
