
'''
Console module to download and upload in the database any kind of historical options data.\n
If -t parameter is passed with a value greater than one the script will use multithreading by splitting tickers in every thread equally.\n
Tickers get loaded from the database metadata table.\n
If --no-ticker-filter flag is passed every ticker in the metadata table gets queried.\n
If --no-ticker-filter flag is NOT passed it will only query tickers from metadata that have in their 'provider' list the chosen provider.\n

Usage:\n
python option_download.py -p <PROVIDER> [-t <THREAD COUNT>, default 1] [--no-ticker-filter]
'''

__autor__ = "Luca Crema <lc.crema@hotmail.com>"
__version__ = "1.2"

import math
import threading
from datetime import date, datetime, timedelta
from typing import List

from sqlalchemy import func

import otri.utils.config as config
import otri.utils.logger as log
from otri.database.postgresql_adapter import PostgreSQLAdapter
from otri.downloader.yahoo_downloader import OptionsDownloader, YahooOptions
from otri.importer.default_importer import DataImporter, DefaultDataImporter
from otri.utils.cli import CLI, CLIValueOpt, CLIFlagOpt

DOWNLOADERS = {
    "YahooFinance": {"class": YahooOptions, "args": {}, "delay": 0}
}
METADATA_TABLE = "metadata"
ATOMS_TABLE = "atoms_b"


class DownloadJob(threading.Thread):
    def __init__(self, tickers: List[str], downloader: OptionsDownloader, importer: DataImporter):
        super().__init__()
        self.shutdown_flag = threading.Event()
        self.tickers = tickers
        self.downloader = downloader
        self.importer = importer

    def run(self):
        start_date = (datetime.now() - timedelta(days=7)).date()
        end_date = date.today()
        for ticker in self.tickers:
            log.i("working on ticker {}".format(ticker))
            # Get the list of expirations
            expirations = self.downloader.expirations(ticker)
            if(expirations is False):
                log.e("unable to retrieve options expiration dates for {}".format(ticker))
                continue

            for expiration in expirations:
                log.i("working on {} expiration date {}".format(ticker, expiration))
                # Download calls
                log.i("downloading calls chain")
                calls = self.downloader.chain(ticker, expiration, "calls")
                if(calls is False):
                    log.e("unable to download {} exp {} calls".format(ticker, expiration))
                    continue
                log.i("downloaded calls chain")
                log.i("attempting to upload {} exp {} calls".format(ticker, expiration))
                self.importer.from_contents(calls)
                log.i("uploaded {} exp {} calls".format(ticker, expiration))

                # Download puts
                log.i("downloading puts chain")
                puts = self.downloader.chain(ticker, expiration, "puts")
                if(puts is False):
                    log.e("unable to download {} exp {} puts".format(ticker, expiration))
                    continue
                log.i("downloaded puts chain")
                log.i("attempting to upload {} exp {} puts".format(ticker, expiration))
                self.importer.from_contents(puts)
                log.i("uploaded {} exp {} puts".format(ticker, expiration))

                # Download last trade history
                log.i("downloading trade history of calls")
                for call_contract in self.downloader.chain_contracts(ticker, expiration, "calls"):
                    log.v("working on contract {}".format(call_contract))
                    history = self.downloader.history(call_contract, start=start_date, end=end_date, interval="1m")
                    if(history is False):
                        log.e("unable to download {} history".format(call_contract))
                        continue
                    log.v("downloaded {} call contract data".format(call_contract))
                    log.v("attempting to upload {} call contract".format(call_contract))
                    self.importer.from_contents(history)
                    log.v("uploaded {} call contract".format(call_contract))

                log.i("downloading trade history of puts")
                for put_contract in self.downloader.chain_contracts(ticker, expiration, "puts"):
                    log.v("working on contract {}".format(put_contract))
                    history = self.downloader.history(put_contract, start=start_date, end=end_date, interval="1m")
                    if(history is False):
                        log.e("unable to download {} history".format(put_contract))
                        continue
                    log.v("downloaded {} put contract data".format(call_contract))
                    log.v("attempting to upload {} put contract".format(call_contract))
                    self.importer.from_contents(history)
                    log.v("uploaded {} put contract".format(call_contract))

            log.i("finished ticker {}".format(ticker))


if __name__ == "__main__":

    cli = CLI(name="option_download",
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
                      long_desc="Avoids filtering tickers from the ticker list by provider and tries to download them all."
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
        user=config.get_value("postgresql_username"),
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
    tickers = list()
    if provider_filter:
        with db_adapter.begin() as conn:
            md_table = db_adapter.get_tables()[METADATA_TABLE]
            query = md_table.select()\
                .where(md_table.c.data_json['provider'].contains('\"{}\"'.format(provider_db_name)))\
                .where(md_table.c.data_json.has_key("ticker"))\
                .order_by(md_table.c.data_json["ticker"].astext)
            for row in conn.execute(query).fetchall():
                tickers.append(row.data_json['ticker'])
    else:
        with db_adapter.begin() as conn:
            md_table = db_adapter.get_tables()[METADATA_TABLE]
            query = md_table.select()\
                .where(func.lower(md_table.c.data_json['type'].astext).in_(['equity', 'index', 'stock', 'etf']))\
                .where(md_table.c.data_json.has_key("ticker"))\
                .order_by(md_table.c.data_json["ticker"].astext)
            for row in conn.execute(query).fetchall():
                tickers.append(row.data_json['ticker'])

    # Reduce console output
    log.min_console_priority = 2

    log.i("beginning options download from provider {}".format(provider))

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

    log.i("download finished")
