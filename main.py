# Modulo per eseguire cose:

from importer.yahoo_json_data_importer import YahooJSONDataImporter
from database.posgresql_adapter import PosgreSQLAdapter
from pathlib import Path
from config import Config

if __name__ == '__main__':
    config = Config()
    database_adapter = PosgreSQLAdapter(config.get_config("postgre_username"), config.get_config("postgre_password"), config.get_config("postgre_host"))
    file_data_importer = YahooJSONDataImporter(database_adapter)
    file_data_importer.from_file(Path("data/AA_27-03-20_to_20-03-20.json"))