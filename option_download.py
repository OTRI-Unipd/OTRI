
'''
Console module to download and upload in the database any kind of historical options data.\n
If -t parameter is passed with a value greater than one the script will use multithreading by splitting tickers in every thread equally.\n
Tickers get loaded from the database metadata table.\n
If --no-ticker-filter flag is passed every ticker in the metadata table gets queried.\n
If --no-ticker-filter flag is NOT passed it will only query tickers from metadata that have in their 'provider' list the chosen provider.\n

Usage:\n
python option_download.py -p <PROVIDER> [-t <THREAD COUNT>, default 1] [--no-ticker-filter]
'''

__author__ = "Luca Crema <lc.crema@hotmail.com>"
__version__ = "1.2"

import math
import queue
import signal
import time
import threading
from datetime import datetime, timedelta
from typing import List

from sqlalchemy import func

import otri.utils.config as config
import otri.utils.logger as log
from otri.database.postgresql_adapter import PostgreSQLAdapter
from otri.downloader.yahoo import OptionsDownloader, YahooOptions
from otri.downloader import ATOMS_KEY
from otri.importer.default_importer import DataImporter, DefaultDataImporter
from otri.utils.cli import CLI, CLIValueOpt, CLIFlagOpt

PROVIDERS = {
    "YahooFinance": {"class": YahooOptions, "args": {}}
}
METADATA_TABLE = "metadata"
ATOMS_TABLE = "atoms_b"


class DownloadWorker(threading.Thread):
    def __init__(self, tickers: List[str], downloader: OptionsDownloader, contents_queue: queue.Queue, start_dt: datetime, end_dt: datetime):
        super().__init__()
        self.tickers = tickers
        self.downloader = downloader
        self.contents_queue = contents_queue
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
            # Get the list of expirations
            expirations = self.downloader.expirations(ticker)
            if expirations is False:
                log.e("unable to retrieve options expiration dates for {}".format(ticker))
                continue

            for expiration in expirations:
                if not self.execute:
                    log.i("stopping expiration")
                    break
                log.i("working on {} expiration date {}".format(ticker, expiration))
                # Download calls
                calls = self.downloader.chain(ticker, expiration, "call")
                if calls is False:
                    log.w("unable to download {} {} calls".format(ticker, expiration))
                    calls = {ATOMS_KEY: []}
                else:
                    log.d("downloaded {} {} calls chain".format(ticker, expiration))
                    self.contents_queue.put({'data': calls})
                # Download puts
                puts = self.downloader.chain(ticker, expiration, "put")
                if puts is False:
                    log.w("unable to download {} {} puts".format(ticker, expiration))
                    puts = {ATOMS_KEY: []}
                else:
                    log.d("downloaded {} {} puts chain".format(ticker, expiration))
                    self.contents_queue.put({'data': puts})
                # Download history of trades
                call_contracts = [contract['contract'] for contract in calls[ATOMS_KEY]]
                puts_contracts = [contract['contract'] for contract in puts[ATOMS_KEY]]
                contracts = call_contracts + puts_contracts
                for contract in contracts:
                    if not self.execute:
                        log.i("stopping contract")
                        break
                    history = self.downloader.history(contract, start=self.start_dt, end=self.end_dt, interval="1m")
                    if history is False:
                        log.w("unable to download {} contract history".format(contract))
                        continue  # Skip to next contract
                    self.contents_queue.put({'data': history})

        log.i("download thread finished (or stopped)")


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
            except queue.Empty:
                # If it waited too long it checks again if it has to stop
                continue
            try:
                self.importer.from_contents(contents['data'])
            except Exception as e:
                log.w("there has been an exception while uploading data: {}".format(e))
                continue
        log.i("stopped uploader worker")


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

    cli = CLI(name="option_download",
              description="Script that downloads option data.",
              options=[
                  CLIValueOpt(
                      short_name="p",
                      long_name="provider",
                      short_desc="Provider",
                      long_desc="Provider for the data.",
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
                      long_desc="Avoids filtering tickers from the ticker list by provider and tries to download them all."
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
        user=config.get_value("postgresql_username"),
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
    contents_queue = queue.Queue(maxsize=20)

    # Prepare start and end date (fixed)
    start_dt = (datetime.now() - timedelta(days=7))
    end_dt = datetime.now()

    log.i("beginning download from provider {}, from {} to {} using {} threads".format(
        provider, start_dt, end_dt, thread_count))

    # Start upload thread
    upload_thread = UploadWorker(importer=importer, contents_queue=contents_queue)
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
                           start_dt=start_dt, end_dt=end_dt)
        threads.append(t)
        try:
            t.start()
        except Exception:
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

    log.i("download completed")
