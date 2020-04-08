# Modulo per eseguire cose:

from importer.yahoo_json_data_importer import YahooJSONDataImporter
from database.posgresql_adapter import PosgreSQLAdapter,DatabaseQuery
from pathlib import Path
from config import Config

def list_jsons(data_path : Path):
    '''
    Lists all json files in a folder
    '''
    docs_path = data_path.rglob('*.json')
    files = [x for x in docs_path if x.is_file()]
    return files

def list_folders(data_path : Path):
    assert(data_path.is_dir())
    folder_list = list()
    for x in data_path.iterdir():
        if x.is_dir():
            folder_list.append(x.name)
    return folder_list

def upload_all_folder_files(folder_path : Path, file_data_importer : YahooJSONDataImporter):
    for json_file_name in list_jsons(folder_path):
        print("Uploading {}".format(json_file_name))
        file_data_importer.from_file(Path(json_file_name))

def choose_path():
    '''
    Shows the user a list of paths and lets him choose where to read data from

    Returns:
        Path chosen
    '''
    downloads_path = Path("../history-downloader/data/")
    source_name = input("Choose between the following services: {} ".format(list_folders(downloads_path)))
    source_path = Path(downloads_path,source_name)
    folder_name = input("Choose between the following downloads: {} ".format(list_folders(source_path)))
    return Path(source_path,folder_name)

if __name__ == '__main__':
    config = Config()
    database_adapter = PosgreSQLAdapter(config.get_config("postgre_username"), config.get_config("postgre_password"), config.get_config("postgre_host"))
    file_data_importer = YahooJSONDataImporter(database_adapter)
    upload_all_folder_files(choose_path(), file_data_importer)        
    #print(database_adapter.read(DatabaseQuery("atoms_j","data_json->>'ticker' = 'AAPL' OR data_json->>'ticker' = 'AA'")))
