import getopt
import json
import math
import sys
import threading
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List

import otri.utils.config as config
import otri.utils.logger as log
from otri.downloader.yahoo_downloader import YahooOptions, OptionsDownloader
from otri.importer.data_importer import DataImporter, DefaultDataImporter
from otri.database.postgresql_adapter import PostgreSQLAdapter

DATA_FOLDER = Path("data/")
TICKER_LISTS_FOLDER = Path("docs/")
DOWNLOADERS = {
    "YahooFinance": {"class": YahooOptions, "args": {}, "delay": 0}
}
METADATA_TABLE = "metadata"
ATOMS_TABLE = "atoms_b"

class DownloadJob(threading.Thread):
    def __init__(self, tickers: List[str], downloader: OptionsDownloader, importer : DataImporter):
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
            expirations = self.downloader.get_expirations(ticker)
            if(expirations == False):
                log.e("unable to retrieve options expiration dates for {}".format(ticker))
                continue

            for expiration in expirations:
                log.i("working on {} expiration date {}".format(ticker, expiration))
                # Download calls
                log.i("downloading calls chain")
                calls = self.downloader.get_chain(ticker, expiration, "calls")
                if(calls == False):
                    log.e("unable to download {} exp {} calls".format(ticker, expiration))
                    continue
                log.i("downloaded calls chain")
                log.i("attempting to upload {} exp {} calls".format(ticker, expiration))
                self.importer.from_contents(calls)
                log.i("uploaded {} exp {} calls".format(ticker, expiration))

                # Download puts
                log.i("downloading puts chain")
                puts = self.downloader.get_chain(ticker, expiration, "puts")
                if(puts == False):
                    log.e("unable to download {} exp {} puts".format(ticker, expiration))
                    continue
                log.i("downloaded puts chain")
                log.i("attempting to upload {} exp {} puts".format(ticker, expiration))
                self.importer.from_contents(puts)
                log.i("uploaded {} exp {} puts".format(ticker, expiration))

                # Download last trade history
                log.i("downloading trade history of calls")
                for call_contract in self.downloader.get_chain_contracts(ticker, expiration, "calls"):
                    log.v("working on contract {}".format(call_contract))
                    history = self.downloader.get_history(call_contract, start=start_date, end=end_date, interval="1m")
                    if(history == False):
                        log.e("unable to download {} history".format(call_contract))
                        continue
                    log.v("downloaded {} call contract data".format(call_contract))
                    log.v("attempting to upload {} call contract".format(call_contract))
                    self.importer.from_contents(history)
                    log.v("uploaded {} call contract".format(call_contract))

                log.i("downloading trade history of puts")
                for put_contract in self.downloader.get_chain_contracts(ticker, expiration, "puts"):
                    log.v("working on contract {}".format(put_contract))
                    history = self.downloader.get_history(put_contract, start=start_date, end=end_date, interval="1m")
                    if(history == False):
                        log.e("unable to download {} history".format(put_contract))
                        continue
                    log.v("downloaded {} put contract data".format(call_contract))
                    log.v("attempting to upload {} put contract".format(call_contract))
                    self.importer.from_contents(history)
                    log.v("uploaded {} put contract".format(call_contract))

            log.i("finished ticker {}".format(ticker))

def print_error_msg(msg: str = None):
    if msg != None:
        msg = msg + ": "

    log.e("{}option_download.py -p <provider: {}> [-t <number of threads, default 1>] [--no-ticker-filter]".format(
        msg,
        list(DOWNLOADERS.keys())
    ))


if __name__ == "__main__":

    if len(sys.argv) < 2:
        print_error_msg("Not enough arguments")
        sys.exit(2)

    provider = ""
    ticker_file = ""
    thread_count = 1
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hp:t:", ["help", "provider=","threads="])
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
        elif opt in ("-t", "--threads"):
            thread_count = int(arg)

    # Check if necessary arguments have been given
    if provider == None:
        print_error_msg("Missing argument provider")
        quit(2)

    # Check if passed arguments are valid
    if not provider in list(DOWNLOADERS.keys()):
        print_error_msg("Provider {} not supported".format(provider))
        quit(2)

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
    provider_db_name = downloader.META_PROVIDER_VALUE
    tickers = list()
    with db_adapter.session() as session:
        md_table = db_adapter.get_tables()[METADATA_TABLE]
        query = session.query(md_table).filter(
            md_table.data_json['provider'].contains(provider_db_name)
        ).filter(
            md_table.data_json.has_key("ticker")
        ).order_by(md_table.data_json["ticker"].astext)
        for row in query.all():
            tickers.append(row['ticker'])

    # Reduce console output
    log.min_console_priority = 2

    log.i("beginning options download from provider {} of tickers {}".format(provider, ticker_file))

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
