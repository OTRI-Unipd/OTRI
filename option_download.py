import json
import math
import threading
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List

import otri.utils.config as config
import otri.utils.logger as log
from otri.database.postgresql_adapter import PostgreSQLAdapter
from otri.downloader.yahoo_downloader import OptionsDownloader, YahooOptionsDW
from otri.importer.data_importer import DataImporter, DefaultDataImporter
from otri.utils.cli import CLI, CLIValueOpt

DATA_FOLDER = Path("data/")
TICKER_LISTS_FOLDER = Path("docs/")
DOWNLOADERS = {
    "YahooFinance": YahooOptionsDW()
}


class DownloadJob(threading.Thread):
    def __init__(self, tickers: List[str], downloader: OptionsDownloader, importer : DataImporter):
        super().__init__()
        self.shutdown_flag = threading.Event()
        self.tickers = tickers
        self.downloader = downloader
        self.importer = importer

    def run(self):
        start_date = get_seven_days_ago()
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

def retrieve_ticker_list(doc_path: Path) -> List[str]:
    '''
    Grabs all tickers from the properly formatted doc_path file.\n

    Returns:\n
        A list of str, names of tickers.\n
    '''
    doc = json.load(doc_path.open("r"))
    return [ticker['ticker'] for ticker in doc['tickers']]


def list_tickers_file(ticker_list_folder: Path) -> Path:
    '''
    List json files from the docs folder where to find the ticker list.

    Returns:
        Path to the selected ticker list file.
    '''
    docs_glob = ticker_list_folder.glob('*.json')
    return [x.name.replace('.json', '') for x in docs_glob if x.is_file()]


def get_seven_days_ago() -> date:
    '''
    Calculates when is 7 days ago.\n

    Returns:\n
        The date of 7 days ago.\n
    '''
    seven_days_delta = timedelta(days=7)
    seven_days_ago = datetime.now() - seven_days_delta
    return date(seven_days_ago.year, seven_days_ago.month, seven_days_ago.day)


def print_error_msg(msg: str = None):
    if msg is None:
        log.e("option_download.py -p <provider: {}> -f <ticker file: {}>".format(
            list(DOWNLOADERS.keys()),
            list_tickers_file(TICKER_LISTS_FOLDER)
        )
        )
    else:
        log.e("{}: option_download.py -p <provider: {}> -f <ticker file: {}>".format(
            msg,
            list(DOWNLOADERS.keys()),
            list_tickers_file(TICKER_LISTS_FOLDER)
        )
        )


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

    provider = values["p"]
    ticker_file = values["f"]
    thread_count = int(values["t"])

    if thread_count < 0:
        thread_count = 1

     # Retrieve the ticker list from the chosen file
    tickers = retrieve_ticker_list(Path(TICKER_LISTS_FOLDER, "{}.json".format(ticker_file)))

    # Setup downloader and timeout time
    downloader = DOWNLOADERS[provider]

    # Setup database connection
    database_adapter = PostgreSQLAdapter(
        config.get_value("postgre_username"),
        config.get_value("postgre_password"),
        config.get_value("postgre_host"))
    importer = DefaultDataImporter(database_adapter)

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
