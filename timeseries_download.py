from pathlib import Path
from otri.downloader.timeseries_downloader import TimeseriesDownloader
from otri.downloader.yahoo_downloader import YahooTimeseriesDW
from otri.downloader.alphavantage_downloader import AVTimeseriesDW
from typing import List, Dict
from datetime import date, datetime, timedelta
from otri.utils.config import Config
import json
import time

DATA_FOLDER = Path("data/")
# downloader : (obj, download delay)
DOWNLOADERS = {
    "YahooFinance": (YahooTimeseriesDW(), 0),
    "AlphaVantage":  (AVTimeseriesDW(Config.get_config("alphavantage_api_key")), 15)
}

TICKER_LISTS_FOLDER = Path("docs/")


def check_and_create_folder(path: Path):
    '''
    Creates data folder if it doesn't exist
    '''
    if(not path.exists()):
        path.mkdir(exist_ok=True)
    return path


def choose_downloader(downloaders_dict : dict) -> str:
    '''
    Choose which downloader to use from the available ones.

    Returns:
        The name of the chosen downloader.
    '''
    while(True):
        choice = input("Choose between: {} ".format(list(downloaders_dict.keys())))
        if(choice in downloaders_dict.keys()):
            break
        print("Unable to parse ", choice)
    return choice


def choose_tickers_file(ticker_list_folder : Path) -> Path:
    '''
    Choose a json file from the docs folder where to find the ticker list.

    Returns:
        Path to the selected ticker list file.
    '''
    docs_glob = ticker_list_folder.glob('*.json')
    doc_list = [x.name.replace('.json', '') for x in docs_glob if x.is_file()]
    while(True):
        choice = input("Select ticker list: {} ".format(doc_list))
        chosen_path = Path(ticker_list_folder, "{}.json".format(choice))
        if(chosen_path.exists()):
            break
        print("Unable to parse ", choice)
    return chosen_path


def retrieve_ticker_list(doc_path: Path) -> List[str]:
    '''
    Grabs all tickers from the properly formatted doc_path file.

    Returns:
        A list of str, names of tickers.
    '''
    doc = json.load(doc_path.open("r"))
    return [ticker['ticker'] for ticker in doc['tickers']]


def write_in_file(path: Path, contents: dict):
    '''
    Writes contents dict data in the given path file.
    '''
    path.open("w+").write(json.dumps(contents, indent=4))


def get_datafolder_name(interval: str, start_date: date, end_date: date) -> str:
    return "{}_from_{}-{}-{}_to_{}-{}-{}".format(
        interval,
        start_date.day,
        start_date.month,
        start_date.year,
        end_date.day,
        end_date.month,
        end_date.year
    )


def get_filename(ticker: str, interval: str, start_date: date, end_date: date) -> str:
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


def get_seven_days_ago() -> date:
    '''
    Calculates when is 7 days ago.

    Returns:
        The date of 7 days ago.
    '''
    seven_days_delta = timedelta(days=7)
    seven_days_ago = datetime.now() - seven_days_delta
    return date(seven_days_ago.year, seven_days_ago.month, seven_days_ago.day)


if __name__ == "__main__":
    # First, let's check if DATA_FOLDER is created
    check_and_create_folder(DATA_FOLDER)
    downloader_name = choose_downloader(DOWNLOADERS)
    downloader = DOWNLOADERS[downloader_name][0]
    service_data_folder = Path(DATA_FOLDER, downloader_name)
    check_and_create_folder(service_data_folder)

    # Choose ticker list file
    ticker_list_path = choose_tickers_file(TICKER_LISTS_FOLDER)

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

    for ticker in tickers:
        print("Working on ", ticker)
        # Prepare the filename
        filename = get_filename(ticker, "1m", start_date, end_date)
        # Actually download data
        downloaded_data = downloader.download_between_dates(
            ticker=ticker, start=start_date, end=end_date, interval="1m")
        if(downloaded_data == False):
            print("Unable to download ", ticker)
            continue
        # Write data in the chosen file
        write_in_file(Path(datafolder, filename), downloaded_data)
        print("OK ", ticker)
        time.sleep(DOWNLOADERS[downloader_name][1])
