'''
Console module to download and upload in the database any kind of historical timeseries data.\n
If -t parameter is passed with a value greater than one the script will use multithreading by splitting tickers in every thread equally.\n
Tickers get loaded from the database metadata table.\n
If --no-ticker-filter flag is passed every ticker in the metadata table gets queried and if successfuly downloaded the metadata
entry gets updated with the chosen provider;
if download was unsuccesfull the provider key won't be removed for safety reasons.\n
If --no-ticker-filter flag is NOT passed it will only query tickers from metadata that have in their 'provider' list the chosen provider.\n
Some provider might have some download limits, therefore a delay system is used to slow down download.\n
Upload of downloaded data is done async in another thread not to slow down download.\n

Usage:\n
python timeseries_download.py -p <PROVIDER> [-t <THREAD COUNT>, default 1] [--no-ticker-filter]
'''

__author__ = "Luca Crema <lc.crema@hotmail.com>"
__version__ = "1.1"

import math
import traceback
import queue
import signal
import threading
import time
from datetime import datetime, timedelta
from typing import List

from sqlalchemy import func

from otri.database.postgresql_adapter import PostgreSQLAdapter
from otri.downloader import TimeseriesDownloader
from otri.downloader.alphavantage_downloader import AVTimeseries
from otri.downloader.yahoo_downloader import YahooTimeseries
from otri.downloader.tradier import TradierTimeseries
from otri.importer.default_importer import DataImporter, DefaultDataImporter
from otri.utils import config
from otri.utils import logger as log
from otri.utils.cli import CLI, CLIFlagOpt, CLIValueOpt

# downloader : (obj, args, download delay)
PROVIDERS = {
    "YahooFinance": {"class": YahooTimeseries, "args": {}},
    "AlphaVantage": {"class": AVTimeseries, "args": {"api_key": config.get_value("alphavantage_api_key")}},
    "Tradier": {"class": TradierTimeseries, "args": {"api_key": config.get_value("tradier_api_key")}}
}
METADATA_TABLE = "metadata"
ATOMS_TABLE = "atoms_b"


class DownloadWorker(threading.Thread):
    def __init__(self, tickers: List[str], downloader: TimeseriesDownloader, contents_queue: queue.Queue, start_dt: datetime, end_dt: datetime, update_provider: bool = False):
        super().__init__()
        self.tickers = tickers
        self.downloader = downloader
        self.contents_queue = contents_queue
        self.update_provider = update_provider
        self.start_dt = start_dt
        self.end_dt = end_dt
        self.execute = True

    def run(self):
        log.i("starting downloader worker")
        for ticker in self.tickers:
            if not self.execute:
                log.i("stopping download thread")
                break
            log.d("downloading {}".format(ticker))
            # Actually download data
            downloaded_data = self.downloader.history(ticker=ticker, start=self.start_dt, end=self.end_dt, interval="1m")
            if downloaded_data is False:
                log.e("unable to download {}".format(ticker))
                continue
            log.d("successfully downloaded {}".format(ticker))
            self.contents_queue.put({'data': downloaded_data, 'update_provider': self.update_provider, 'ticker': ticker})
        log.i("download thread finished (or stopped)")


class UploadWorker(threading.Thread):
    '''
    Waits for data to pop in the passed contents_queue
    '''

    def __init__(self, importer: DataImporter, contents_queue: queue.Queue, metadata_table, provider_name: str):
        super().__init__()
        self.contents_queue = contents_queue
        self.importer = importer
        self.md_table = metadata_table
        self.provider_name = provider_name

    def run(self):
        log.i("started uploader worker")
        self.execute = True
        while(self.execute or self.contents_queue.qsize() != 0):
            # Wait at most <timeout> seconds for something to pop in the queue
            try:
                contents = self.contents_queue.get(block=True, timeout=5)
            except Exception:
                # If it waited too long it checks again if it has to stop
                continue
            ticker = contents['ticker']
            log.d("attempting to upload {} contents".format(ticker))
            try:
                self.importer.from_contents(contents['data'])
            except Exception as e:
                log.w("there has been an exception while uploading data: {}".format(e))
                continue
            log.d("successfully uploaded {} contents".format(ticker))
            if contents['update_provider']:
                log.d("attempting to update {} provider".format(ticker))
                try:
                    self.update_provider(contents['ticker'])
                except Exception as e:
                    log.w("there has been an exception while updating provider: {}".format(e))
                    continue
                log.d("successfully updated {} provider".format(ticker))
        log.i("stopped uploader worker")

    def update_provider(self, ticker: str):
        with self.importer.database.session() as session:
            md_row = session.query(self.md_table).filter(
                self.md_table.c.data_json['ticker'].astext == ticker
            ).one()
            if('provider' not in md_row.data_json):
                md_row.data_json['provider'] = []
            md_row.data_json['provider'].append(self.provider_name)


def kill_threads(signum, frame):
    log.w("gracefully stopping all threads")
    global threads
    global upload_thread
    global execute
    upload_thread.execute = False
    for dw_thread in threads:
        dw_thread.execute = False
    execute = False


def threads_running(threads):
    for t in threads:
        if t.is_alive():
            return True
    return False


if __name__ == "__main__":

    signal.signal(signal.SIGINT, kill_threads)
    signal.signal(signal.SIGTERM, kill_threads)

    cli = CLI(name="timeseries_download",
              description="Script that downloads weekly historical timeseries data.",
              options=[
                  CLIValueOpt(
                      short_name="p",
                      long_name="provider",
                      short_desc="Provider",
                      long_desc="Provider for the historical data.",
                      required=True,
                      values=list(PROVIDERS.keys())
                  ),
                  CLIValueOpt(
                      short_name="t",
                      long_name="threads",
                      short_desc="Threads",
                      long_desc="Number of threads where tickers will be downloaded in parallel.",
                      required=False,
                      default="1"
                  ),
                  CLIFlagOpt(
                      long_name="no-provider-filter",
                      short_desc="Do not filter tickers by provider",
                      long_desc="Avoids filtering tickers from the ticker list by provider and tries to download them all. If it could download a ticker it updates its provider."
                  )
              ])

    values = cli.parse()
    provider = values["-p"]
    thread_count = int(values["-t"])
    provider_filter = not values["--no-provider-filter"]

    # Fix thread count if invalid
    if thread_count < 0:
        thread_count = 1

    # Setup database connection
    db_adapter = PostgreSQLAdapter(
        host=config.get_value("postgresql_host"),
        port=config.get_value("postgresql_port", "5432"),
        user=config.get_value("postgresql_username", "postgres"),
        password=config.get_value("postgresql_password"),
        database=config.get_value("postgresql_database", "postgres")
    )
    importer = DefaultDataImporter(db_adapter)

    # Setup downloader class and args
    args = PROVIDERS[provider]['args']
    dw_class = PROVIDERS[provider]['class']
    downloader = dw_class(**args, limiter=dw_class.DEFAULT_LIMITER)

    # Query the database for a ticker list
    provider_db_name = downloader.provider_name
    tickers = []
    md_table = db_adapter.get_tables()[METADATA_TABLE]
    if provider_filter:
        with db_adapter.begin() as connection:
            query = md_table.select().where(
                md_table.c.data_json['provider'].contains('\"{}\"'.format(provider_db_name))
            ).where(
                md_table.c.data_json.has_key("ticker")
            ).order_by(md_table.c.data_json["ticker"].astext)
            for row in connection.execute(query).fetchall():
                tickers.append(row.data_json['ticker'])
    else:
        with db_adapter.begin() as connection:
            query = md_table.select().where(
                func.lower(md_table.c.data_json['type'].astext).in_(['equity', 'index', 'stock', 'etf'])
            ).where(
                md_table.c.data_json.has_key("ticker")
            ).order_by(md_table.c.data_json["ticker"].astext)
            for row in connection.execute(query).fetchall():
                tickers.append(row.data_json['ticker'])

    log.i("found {} tickers to download".format(len(tickers)))

    # Setup contents_queue, the upload can be behind download of at most 10 elements
    contents_queue = queue.Queue(maxsize=10)

    # Prepare start and end date (fixed)
    start_dt = (datetime.now() - timedelta(days=7))
    end_dt = datetime.now()

    log.i("beginning download from provider {}, from {} to {} using {} threads".format(
        provider, start_dt, end_dt, thread_count))

    # Start upload thread
    upload_thread = UploadWorker(importer=importer, contents_queue=contents_queue, metadata_table=md_table, provider_name=downloader.provider_name)
    upload_thread.start()

    # Multithreading
    threads = list()

    # Split ticker in threads_count groups
    if(thread_count <= 0):
        thread_count = math.sqrt(len(tickers))
        log.d("updating numeber of threads to {}".format(thread_count))
    n = round(len(tickers) / thread_count) + 1
    ticker_groups = [tickers[i:i + n] for i in range(0, len(tickers), n)]
    log.i("splitting in {} threads with {} tickers".format(len(ticker_groups), [len(group) for group in ticker_groups]))

    # Start the jobs
    for t_group in ticker_groups:
        t = DownloadWorker(tickers=t_group, downloader=downloader, contents_queue=contents_queue,
                           start_dt=start_dt, end_dt=end_dt, update_provider=not provider_filter)
        threads.append(t)
        try:
            t.start()
        except Exception as e:
            log.w("error starting download thread: {}: {}".format(e, traceback.print_exc()))
            upload_thread.execute = False

    log.i("main thread waiting - sleeping")
    execute = True
    while(execute):
        if(threads_running(threads)):
            time.sleep(1)
            continue
        kill_threads(None, None)

    # Waiting for threads to finish
    log.d("waiting for download threads")
    for thread in threads:
        if thread.is_alive():
            thread.join()
    log.d("all download threads stopped")

    log.d("waiting for upload thread")
    upload_thread.join()

    log.i("download completed")
