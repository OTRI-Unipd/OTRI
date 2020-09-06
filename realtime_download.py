'''
Module that uses Realtime downloader to download and upload realtime trades.
'''

__author__ = "Luca Crema <lc.crema@hotmail.com>"
__version__ = "1.1"

import queue
import signal
import threading
import time
from typing import Sequence

from sqlalchemy import func

from otri.database.postgresql_adapter import PostgreSQLAdapter
from otri.downloader import RealtimeDownloader
from otri.downloader.tradier import TradierRealtime
from otri.importer.default_importer import DataImporter, DefaultDataImporter
from otri.utils import config
from otri.utils import logger as log
from otri.utils.cli import CLI, CLIValueOpt, CLIFlagOpt

PROVIDERS = {
    "Tradier": {"class": TradierRealtime, "args": {"key": config.get_value("tradier_api_key")}}
}
METADATA_TABLE = "metadata"
ATOMS_TABLE = "atoms_b"


class UploadWorker(threading.Thread):
    '''
    Waits for data to pop in the passed contents_queue
    '''

    def __init__(self, importer: DataImporter, contents_queue: queue.Queue):
        super().__init__()
        self.contents_queue = contents_queue
        self.importer = importer

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
            try:
                self.importer.from_contents(contents['data'])
            except Exception as e:
                log.w("there has been an exception while uploading data: {}".format(e))
                continue
            log.d("successfully uploaded contents")
        log.i("stopped uploader worker")


class DownloadWorker(threading.Thread):
    def __init__(self, downloader: RealtimeDownloader, tickers: Sequence[str], contents_queue: queue.Queue):
        super().__init__()
        self.downloader = downloader
        self.tickers = tickers
        self.contents_queue = contents_queue

    def run(self):
        log.i("started downloader worker")
        self.downloader.start(self.tickers, self.contents_queue)
        log.i("stopped downloader worker")


def kill_threads(signum, frame):
    log.i("stopping threads")
    global threads
    global upload_thread
    global execute
    upload_thread.execute = False
    for dw_thread in threads:
        dw_thread.downloader.stop()
    execute = False


def threads_running(threads):
    for t in threads:
        if t.is_alive():
            return True
    return False


if __name__ == "__main__":

    signal.signal(signal.SIGINT, kill_threads)
    signal.signal(signal.SIGTERM, kill_threads)

    cli = CLI(name="realtime_download",
              description="Script that downloads realtime data continuosly.",
              options=[
                  CLIValueOpt(
                      short_name="p",
                      long_name="provider",
                      short_desc="Provider",
                      long_desc="Provider for the historical data.",
                      required=True,
                      values=list(PROVIDERS.keys())
                  ),
                  CLIFlagOpt(
                      long_name="no-provider-filter",
                      short_desc="Do not filter tickers by provider",
                      long_desc="Avoids filtering tickers from the ticker list by provider and tries to download them all."
                  )
              ])

    # Get cli options
    values = cli.parse()
    provider_name = values["-p"]
    provider_filter = not values["--no-provider-filter"]

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
    args = PROVIDERS[provider_name]['args']
    dw_class = PROVIDERS[provider_name]['class']
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

    # Setup contents_queue, the upload can be behind download of at most 10 elements
    contents_queue = queue.Queue(maxsize=10)

    # Start upload thread
    upload_thread = UploadWorker(importer=importer, contents_queue=contents_queue)
    upload_thread.start()

    # Split tickers
    n = 1000
    ticker_groups = [tickers[i:i + n] for i in range(0, len(tickers), n)]

    log.i("splitting in {} threads with {} tickers".format(
        len(ticker_groups), [len(group) for group in ticker_groups]))

    # Multithreading
    threads = list()

    for t_group in ticker_groups:
        t = DownloadWorker(tickers=t_group, downloader=downloader, contents_queue=contents_queue)
        threads.append(t)
        try:
            t.start()
        except Exception as e:
            log.w("error starting download thread: {}".format(e))
            kill_threads(None, None)

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
    log.i("terminated realtime download script")
