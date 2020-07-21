from typing import List, Dict
from pathlib import Path
from datetime import date, datetime, timedelta
from otri.downloader.yahoo_downloader import YahooOptionsDW
import json
import threading
import otri.utils.logger as log
import otri.utils.config as config
import signal
import time

DATA_FOLDER = Path("data/")
TICKER_LISTS_FOLDER = Path("docs/")
DOWNLOADERS = {
    "YahooFinance": YahooOptionsDW()
}

class Job(threading.Thread):
    def __init__(self, ticker : str):
        super().__init__()
        self.shutdown_flag = threading.Event()
        self.ticker = ticker

    def run(self):
        log.i("Working on ticker {}".format(self.ticker))
        # Get the list of expirations
        expirations = downloader.get_expirations(self.ticker)
        if(expirations == False):
            log.e("Unable to retrieve options expiration dates for {}".format(self.ticker))
            return

        for expiration in expirations:
            log.i("Working on expiration date {}".format(expiration))
            # Download calls
            log.i("Downloading calls chain")
            calls_filename = get_chain_filename(self.ticker, expiration, "calls")
            calls = downloader.get_chain(self.ticker, expiration, "calls")
            if(calls == False):
                log.e("Unable to download {} exp {}".format(self.ticker, expiration))
                continue
            write_in_file(Path(datafolder, calls_filename), calls)

            # Download puts
            log.i("Downloading puts chain")
            puts_filename = get_chain_filename(self.ticker, expiration, "calls")
            puts = downloader.get_chain(self.ticker, expiration, "calls")
            if(puts == False):
                log.e("Unable to download {} exp {}".format(self.ticker, expiration))
                continue
            write_in_file(Path(datafolder, puts_filename), puts)

            # Download last trade history
            log.i("Downloading trade history of calls")
            for call_contract in downloader.get_chain_contracts(self.ticker, expiration, "calls"):
                log.v("Working on contract {}".format(call_contract))
                history_filename = get_history_filename(call_contract, "1m", start_date, end_date)
                history = downloader.get_history(call_contract, start=start_date, end=end_date, interval="1m")
                if(history == False):
                    log.e("Unable to download {} history".format(call_contract))
                    continue
                write_in_file(Path(datafolder, history_filename), history)

            log.i("Downloading trade history of puts")
            for put_contract in downloader.get_chain_contracts(self.ticker, expiration, "puts"):
                log.v("Working on contract {}".format(put_contract))
                history_filename = get_history_filename(put_contract, "1m", start_date, end_date)
                history = downloader.get_history(put_contract, start=start_date, end=end_date, interval="1m")
                if(history == False):
                    log.e("Unable to download {} history".format(put_contract))
                    continue
                write_in_file(Path(datafolder, history_filename), history)

def service_shutdown(signum, frame):
    print('Caught signal %d' % signum)
    raise ServiceExit

def check_and_create_folder(path: Path):
    '''
    Creates data folder if it doesn't exist
    '''
    if(not path.exists()):
        path.mkdir(exist_ok=True)
    return path

def choose_downloader(downloaders_dict : dict) -> str:
    '''
    Choose which downloader to use from the available ones.\n

    Returns:\n
        The name of the chosen downloader.\n
    '''
    while(True):
        choice = input("Choose between: {} ".format(list(downloaders_dict.keys())))
        if(choice in downloaders_dict.keys()):
            break
        print("Unable to parse ", choice)
    return choice

def choose_tickers_file(ticker_list_folder : Path) -> Path:
    '''
    Choose a json file from the docs folder where to find the ticker list.\n

    Returns:\n
        Path to the selected ticker list file.\n
    '''
    docs_glob = ticker_list_folder.glob('*.json')
    doc_list = [x.name.replace('.json', '') for x in docs_glob if x.is_file()]
    while(True):
        choice = input("Select ticker list: {} ".format(doc_list))
        chosen_path = Path(ticker_list_folder, "{}.json".format(choice))
        if(chosen_path.exists()):
            break
        log.i("Unable to parse {}".format(choice))
    return chosen_path

def retrieve_ticker_list(doc_path: Path) -> List[str]:
    '''
    Grabs all tickers from the properly formatted doc_path file.\n

    Returns:\n
        A list of str, names of tickers.\n
    '''
    doc = json.load(doc_path.open("r"))
    return [ticker['ticker'] for ticker in doc['tickers']]

def write_in_file(path: Path, contents: dict):
    '''
    Writes contents dict data in the given path file.
    '''
    log.v("writing {} file".format(path))
    path.open("w+").write(json.dumps(contents, indent=4))

def get_datafolder_name(interval: str, start_date: date, end_date: date) -> str:
    return "{}_options_from_{}-{}-{}_to_{}-{}-{}".format(
        interval,
        start_date.day,
        start_date.month,
        start_date.year,
        end_date.day,
        end_date.month,
        end_date.year
    )


def get_history_filename(ticker: str, interval: str, start_date: date, end_date: date) -> str:
    return "{}_{}_from_{}-{}-{}_to_{}-{}-{}.json".format(
        ticker,
        interval,
        start_date.day,
        start_date.month,
        start_date.year,
        end_date.day,
        end_date.month,
        end_date.year
    )

def get_chain_filename(ticker : str, expiration : str, kind : str) -> str:
    return "{}_{}_{}.json".format(ticker, kind, expiration)

def get_seven_days_ago() -> date:
    '''
    Calculates when is 7 days ago.\n

    Returns:\n
        The date of 7 days ago.\n
    '''
    seven_days_delta = timedelta(days=7)
    seven_days_ago = datetime.now() - seven_days_delta
    return date(seven_days_ago.year, seven_days_ago.month, seven_days_ago.day)

class ServiceExit(Exception):
    """
    Custom exception which is used to trigger the clean exit
    of all running threads and the main program.
    """
    pass

if __name__ == "__main__":

    signal.signal(signal.SIGTERM, service_shutdown)
    signal.signal(signal.SIGINT, service_shutdown)

    # First, let's check if DATA_FOLDER is created
    check_and_create_folder(DATA_FOLDER)
    downloader_name = choose_downloader(DOWNLOADERS)
    downloader = DOWNLOADERS[downloader_name]
    service_data_folder = Path(DATA_FOLDER, downloader_name)
    check_and_create_folder(service_data_folder)

    # Choose ticker list file
    ticker_list_path = choose_tickers_file(TICKER_LISTS_FOLDER)

    log.i("beginning download")

    # Create a subfolder named like the chosen file
    ticker_list_data_folder = Path(
        service_data_folder, ticker_list_path.name.replace('.json', ''))
    check_and_create_folder(ticker_list_data_folder)

    # Retrieve the ticker list from the chosen file
    tickers = retrieve_ticker_list(ticker_list_path)
    start_date = get_seven_days_ago()
    end_date = date.today()

    # Create a folder inside data/service_name/ticker_list_filename/ with a proper name
    datafolder = Path(ticker_list_data_folder, get_datafolder_name(
        "1m", start_date=start_date, end_date=end_date))
    check_and_create_folder(datafolder)

    # Reduce console output
    log.min_console_priority = 4

    # Multithreading
    threads = []

    for ticker in tickers:
        t = Job(ticker)
        threads.append(t)
        t.start()

    try:
        while True:
            time.sleep(1)
    except ServiceExit:
        for thread in threads:
            thread.shutdown_flag.set()
        for thread in threads:
            thread.join()

    log.i("Download finished")