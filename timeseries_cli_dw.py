'''
Console module to download and upload any kind of historical timeseries data.\n
If -t parameter is passed with a value greater than one the script will use multithreading by splitting tickers in every thread equally.\n
Tickers get loaded from the database metadata table.\n
If --no-ticker-filter flag is passed every ticker in the metadata table gets queried and if successfuly downloaded the metadata entry gets updated with the chosen provider;
if download was unsuccesfull the provider key won't be removed for safety reasons.\n
If --no-ticker-filter flag is NOT passed it will only query tickers from metadata that have in their 'provider' list the chosen provider.\n
Some provider might have some download limits, therefore a delay system is used to slow down download.\n
Upload of downloaded data is done async in another thread not to slow down download.\n

Usage:\n
python timeseries_cli_dw.py -p <PROVIDER> [-t <THREAD COUNT>, default 1] [--no-ticker-filter]
'''

__autor__ = "Luca Crema <lc.crema@hotmail.com>"
__version__ = "1.1"

import getopt
import json
import sys
import time
import threading
import math
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List

from otri.utils import config, logger as log
from otri.database.postgresql_adapter import PostgreSQLAdapter, DatabaseQuery, DatabaseData
from otri.downloader.alphavantage_downloader import AVTimeseriesDW
from otri.downloader.timeseries_downloader import TimeseriesDownloader
from otri.downloader.yahoo_downloader import YahooTimeseries
from otri.importer.data_importer import DataImporter, DefaultDataImporter


DATA_FOLDER = Path("data/")
# downloader : (obj, args, download delay)
DOWNLOADERS = {
    "YahooFinance": {"class": YahooTimeseries, "args": {}, "delay": 0},
    "AlphaVantage":  {"class": AVTimeseriesDW, "args": {"api_key": config.get_value("alphavantage_api_key")}, "delay": 15}
}
TICKER_LISTS_FOLDER = Path("docs/")


class DownloadJob(threading.Thread):
    def __init__(self, tickers: List[str], downloader: TimeseriesDownloader, timeout_time: float, importer: DataImporter, update_provider: bool = False):
        super().__init__()
        self.shutdown_flag = threading.Event()
        self.tickers = tickers
        self.downloader = downloader
        self.importer = importer
        self.timeout_time = timeout_time
        self.update_provider = update_provider

    def run(self):
        for ticker in self.tickers:
            log.d("downloading {}".format(ticker))
            # Actually download data
            downloaded_data = self.downloader.download_between_dates(
                ticker=ticker, start=start_date, end=end_date, interval="1m")
            log.d("successfully downloaded {}".format(ticker))
            if(downloaded_data == False):
                log.e("unable to download {}".format(ticker))
                time.sleep(self.timeout_time)
                continue
            # Create upload thread and launch it
            upload_job = UploadJob(importer, downloaded_data, ticker, self.update_provider, self.downloader.META_PROVIDER_VALUE)
            upload_job.start()
            # Sleep if required
            if self.timeout_time > 0:
                time.sleep(self.timeout_time)


class UploadJob(threading.Thread):
    def __init__(self, importer: DataImporter, downloaded_data: dict, ticker: str, update_provider: bool = False, provider_name: str = None):
        super().__init__()
        self.downloaded_data = downloaded_data
        self.importer = importer
        self.update_provider = update_provider
        self.ticker = ticker
        self.provider_name = provider_name

    def run(self):
        # Upload data
        log.d("attempting to upload {}".format(self.ticker))
        self.importer.from_contents(self.downloaded_data)
        if self.update_provider:
            log.v("updating ticker provider...")
            # TODO: Update this with an UPDATE and not an INSERT (when db adapter gets refactored)
            self.importer.database.write(DatabaseData("metadata", {"ticker": self.ticker, "provider": [self.provider_name]}))
            log.v("updated ticker provider")
        log.d("successfully uploaded {}".format(self.ticker))


def print_error_msg(msg: str = None):
    if not msg is None:
        msg = msg + ": "

    log.e("{}timeseries_cli_download.py -p <provider: {}> [-t <number of threads, default 1>] [--no-ticker-filter]".format(
        msg,
        list(DOWNLOADERS.keys())
    ))


if __name__ == "__main__":

    if len(sys.argv) < 1:
        print_error_msg("Not enough arguments")
        quit(2)

    provider = None
    thread_count = 1
    ticker_filter = True
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hp:t:", ["help", "provider=", "threads=", "no-ticker-filter"])
    except getopt.GetoptError as e:
        # If the passed option is not in the list it throws error
        print_error_msg(str(e))
        quit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print_error_msg()
            quit()
        elif opt in ("-p", "--provider"):
            provider = arg
        elif opt in ("-t", "--threads"):
            thread_count = int(arg)
        elif opt in ("--no-ticker-filter"):
            # Avoids filtering tickers for "provider" and updates itself as provider for that ticker if it was able to download it
            ticker_filter = False

    # Check if necessary arguments have been given
    if provider == None:
        print_error_msg("Missing argument provider")
        quit(2)

    # Check if passed arguments are valid
    if not provider in list(DOWNLOADERS.keys()):
        print_error_msg("Provider {} not supported".format(provider))
        quit(2)

    # Setup database connection
    database_adapter = PostgreSQLAdapter(
        host=config.get_value("postgresql_host"),
        port=config.get_value("postgresql_port", "5432"),
        user=config.get_value("postgresql_username", "postgres"),
        password=config.get_value("postgresql_password"),
        database=config.get_value("postgresql_database", "postgres")
    )
    importer = DefaultDataImporter(database_adapter)

    # Setup downloader and timeout time
    args = DOWNLOADERS[provider]['args']
    downloader = DOWNLOADERS[provider]['class'](**args)
    timeout_time = DOWNLOADERS[provider]['delay']

    # Query the database for a ticker list
    provider_db_name = downloader.META_PROVIDER_VALUE
    if ticker_filter:
        tickers_metadata = database_adapter.read(DatabaseQuery(
            "metadata", "data_json->'provider' @> '\"{}\"' AND data_json?'ticker' ORDER BY data_json->>'ticker'".format(provider_db_name)))
    else:
        tickers_metadata = database_adapter.read(DatabaseQuery(
            "metadata", "lower(data_json->>'type') IN ('equity','etf','index','stock') AND data_json?'ticker' ORDER BY data_json->>'ticker'"))
    try:
        tickers = [t['ticker'] for t in tickers_metadata]
    except KeyError as e:
        log.e("missing '{}' field in metadata atoms (???): {}".format(e, tickers_metadata))

    # Prepare start and end date (fixed)
    start_date = (datetime.now() - timedelta(days=7)).date()
    end_date = date.today()

    log.i("beginning download from provider {}, from {} to {}".format(
        provider, start_date, end_date))

    # Multithreading
    threads = []

    # Split ticker in threads_count groups
    if(thread_count <= 0):
        thread_count = math.sqrt(len(tickers))
    n = round(len(tickers)/thread_count)
    ticker_groups = [tickers[i:i + n] for i in range(0, len(tickers), n)]
    log.i("splitting in {} threads with {} tickers each".format(len(ticker_groups), n))

    # Start the jobs
    for t_group in ticker_groups:
        t = DownloadJob(t_group, downloader, timeout_time, importer, not ticker_filter)
        threads.append(t)
        # Multithread only if number of threads is more than 1
        if thread_count > 1:
            t.start()
        else:
            t.run()

    # Waiting for threads to finish
    if thread_count > 1:
        for thread in threads:
            thread.join()

    log.i("download completed")
