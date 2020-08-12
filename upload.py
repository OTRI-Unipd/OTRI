# Modulo per eseguire cose:

from pathlib import Path

import otri.utils.config as config
import otri.utils.logger as log
from otri.database.postgresql_adapter import PostgreSQLAdapter
from otri.importer import DataImporter
from otri.importer.default_importer import DefaultDataImporter

# The script assumes the directories are named after the keys of this dictionary.
PROVIDERS = [
    "AlphaVantage",
    "YahooFinance"
]
DOWNLOADS_PATH = Path("data/")


def list_jsons(data_path: Path):
    '''
    Lists all json files in a folder
    '''
    return [x for x in data_path.glob('*.json') if x.is_file()]


def list_folders(data_path: Path):
    assert(data_path.is_dir())
    folder_list = list()
    for x in data_path.iterdir():
        if x.is_dir():
            folder_list.append(x.name)
    return folder_list


def upload_all_folder_files(folder_path: Path, file_default_importer: DataImporter):
    for json_file_name in list_jsons(folder_path):
        log.i("attempting to upload {}".format(json_file_name))
        file_default_importer.from_json_file(Path(json_file_name))
        log.i("successfully uploaded {}".format(json_file_name))


def choose_provider() -> str:
    '''
    Shows a list of data sources (previously downloaded) and has the user choose.

    Returns:
        The name of the chosen provider if a data importer is available. Or None if not available.
    '''
    while(True):
        provider_name = input("Choose between the following services: {} ".format(
            list_folders(DOWNLOADS_PATH)
        ))
        if provider_name in PROVIDERS:
            break
        log.i("Unable to parse {}".format(provider_name))
    return provider_name


def choose_path(sub_dir: Path) -> Path:
    '''
    Shows the user a list of paths and lets him choose where to read data from

    Returns:
        Path chosen
    '''
    while(True):
        choice = input("Choose between the following folders: {} ".format(
            list_folders(sub_dir)
        ))
        chosen_folder = Path(sub_dir, choice)
        if(chosen_folder.exists()):
            break
        log.i("Unable to parse {}".format(choice))
    return chosen_folder


if __name__ == '__main__':

    database_adapter = PostgreSQLAdapter(
        host=config.get_value("postgresql_host"),
        port=config.get_value("postgresql_port"),
        user=config.get_value("postgresql_username"),
        password=config.get_value("postgresql_password"),
        database=config.get_value("postgresql_database")
    )

    provider = choose_provider()
    importer = DefaultDataImporter(database_adapter)
    ticker_list_path = choose_path(Path(DOWNLOADS_PATH, provider))
    downloaded_tickers_path = choose_path(ticker_list_path)
    upload_all_folder_files(downloaded_tickers_path, importer)
