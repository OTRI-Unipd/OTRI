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
import threading
import time
from datetime import date, datetime, timedelta
from typing import List
from sqlalchemy import func

from otri.utils import config, logger as log
from otri.database.postgresql_adapter import PostgreSQLAdapter
from otri.downloader.alphavantage_downloader import AVTimeseries
from otri.downloader import TimeseriesDownloader
from otri.downloader.yahoo_downloader import YahooTimeseries
from otri.importer.default_importer import DataImporter, DefaultDataImporter
from otri.utils.cli import CLI, CLIValueOpt, CLIFlagOpt


# downloader : (obj, args, download delay)
DOWNLOADERS = {
    "YahooFinance": {"class": YahooTimeseries, "args": {}, "delay": 0},
    "AlphaVantage": {"class": AVTimeseries, "args": {"api_key": config.get_value("alphavantage_api_key")}, "delay": 15}
}
METADATA_TABLE = "metadata"
ATOMS_TABLE = "atoms_b"


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
            downloaded_data = self.downloader.history(
                ticker=ticker, start=start_date, end=end_date, interval="1m")
            log.d("successfully downloaded {}".format(ticker))
            if downloaded_data is False:
                log.e("unable to download {}".format(ticker))
                time.sleep(self.timeout_time)
                continue
            # Create upload thread and launch it
            upload_job = UploadJob(importer, downloaded_data, ticker, self.update_provider, self.downloader.META_VALUE_PROVIDER)
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
        self.importer.from_contents(self.downloaded_data, database_table=ATOMS_TABLE)
        if self.update_provider:
            log.d("updating ticker provider...")
            with self.importer.database.session() as session:
                md_table = self.importer.database.get_classes()[METADATA_TABLE]
                md_row = session.query(md_table).filter(
                    md_table.data_json['ticker'].astext == self.ticker
                ).one()
                if('provider' not in md_row.data_json):
                    md_row.data_json['provider'] = []
                md_row.data_json['provider'].append(self.provider_name)
            log.d("updated ticker provider")
        log.d("successfully uploaded {}".format(self.ticker))


if __name__ == "__main__":
    cli = CLI(name="timeseries_download",
              description="Script that downloads weekly historical timeseries data.",
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

    # Setup downloader and timeout time
    args = DOWNLOADERS[provider]['args']
    downloader = DOWNLOADERS[provider]['class'](**args)
    timeout_time = DOWNLOADERS[provider]['delay']

    # Query the database for a ticker list
    provider_db_name = downloader.META_VALUE_PROVIDER
    tickers = []
    if provider_filter:
        with db_adapter.begin() as connection:
            md_table = db_adapter.get_tables()[METADATA_TABLE]
            query = md_table.select().where(
                md_table.c.data_json['provider'].contains('\"{}\"'.format(provider_db_name))
            ).where(
                md_table.c.data_json.has_key("ticker")
            ).order_by(md_table.c.data_json["ticker"].astext)
            for row in connection.execute(query).fetchall():
                tickers.append(row.data_json['ticker'])
    else:
        with db_adapter.begin() as connection:
            md_table = db_adapter.get_tables()[METADATA_TABLE]
            query = md_table.select().where(
                func.lower(md_table.c.data_json['type'].astext).in_(['equity', 'index', 'stock', 'etf'])
            ).where(
                md_table.c.data_json.has_key("ticker")
            ).order_by(md_table.c.data_json["ticker"].astext)
            for row in connection.execute(query).fetchall():
                tickers.append(row.data_json['ticker'])

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
    n = round(len(tickers) / thread_count)
    ticker_groups = [tickers[i:i + n] for i in range(0, len(tickers), n)]
    log.i("splitting in {} threads with {} tickers each".format(len(ticker_groups), n))

    # Start the jobs
    for t_group in ticker_groups:
        t = DownloadJob(t_group, downloader, timeout_time, importer, not provider_filter)
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
