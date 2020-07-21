from pathlib import Path
from datetime import date, datetime, timedelta
from otri.utils.config import Config
import json

DOWNLOADERS = {
    "YahooFinance": (YahooTimeseriesDW(), 0),
    "AlphaVantage":  (AVTimeseriesDW(Config.get_config("alphavantage_api_key")), 15)
}

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