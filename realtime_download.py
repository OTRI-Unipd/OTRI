'''
Module that uses Realtime downloader to download and upload realtime trades.
'''

__autor__ = "Luca Crema <lc.crema@hotmail.com>"
__version__ = "1.0"

import queue
import signal
import threading
import time
from typing import Sequence

from sqlalchemy import func

from otri.database.postgresql_adapter import PostgreSQLAdapter
from otri.downloader import RealtimeDownloader
from otri.downloader.tradier import TradierRealtime
from otri.importer.default_importer import DataImporter, DefaultImporter
from otri.utils import config
from otri.utils import logger as log
from otri.utils.cli import CLI, CLIValueOpt

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
        log.d("started uploader worker")
        self.execute = True
        while(self.execute or self.contents_queue.qsize() != 0):
            # Wait at most <timeout> seconds for something to pop in the queue
            contents = self.contents_queue.get(block=True, timeout=15)
            log.d("attempting to upload contents")
            self.importer.from_contents(contents)
            log.d("successfully uploaded contents")
        log.d("stopped uploader worker")


class DownloadWorker(threading.Thread):
    def __init__(self, downloader: RealtimeDownloader, tickers: Sequence[str], period: float, contents_queue: queue.Queue):
        super().__init__()
        self.downloader = downloader
        self.tickers = tickers
        self.period = period
        self.contents_queue = contents_queue

    def run(self):
        log.d("started downloader worker")
        downloader.start(self.tickers, self.period, self.contents_queue)


def kill_threads(signum, frame):
    log.i("stopping threads")
    global threads
    global upload_thread
    upload_thread.execute = False
    for dw_thread in threads:
        dw_thread.downloader.stop()
    downloader.stop()


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
                  )
              ])

    # Get cli options
    values = cli.parse()
    provider_name = values["-p"]

    # Setup database connection
    db_adapter = PostgreSQLAdapter(
        host=config.get_value("postgresql_host"),
        port=config.get_value("postgresql_port", "5432"),
        user=config.get_value("postgresql_username", "postgres"),
        password=config.get_value("postgresql_password"),
        database=config.get_value("postgresql_database", "postgres")
    )
    importer = DefaultImporter(db_adapter)

    # Get tickers
    tickers = []
    with db_adapter.session() as session:
        md_table = db_adapter.get_tables()[METADATA_TABLE]
        query = session.query(md_table).filter(
            func.lower(md_table.data_json['type'].astext).in_(['equity', 'index', 'stock'])
        ).filter(
            md_table.data_json.has_key("ticker")
        ).order_by(md_table.data_json["ticker"].astext)
        for row in query.all():
            tickers.append(row.data_json['ticker'])

    # Setup contents_queue
    contents_queue = queue.Queue()

    # Start upload thread
    upload_thread = UploadWorker(importer=importer, contents_queue=contents_queue)
    upload_thread.start()

    # Split tickers
    n = 1000
    ticker_groups = [tickers[i:i + n] for i in range(0, len(tickers), n)]
    log.i("splitting in {} threads with {} tickers each".format(len(ticker_groups), n))

    # Calculate period
    period = len(ticker_groups)/2  # where 2 is requests per second

    # Setup downloader threads
    threads = list()
    args = PROVIDERS[provider_name]['args']
    for group in ticker_groups:
        downloader = PROVIDERS[provider_name]['class'](**args)
        dw_thread = DownloadWorker(downloader, group, period, contents_queue)
        threads.append(dw_thread)
        dw_thread.start()

    while(True):
        time.sleep(1)
