# Modulo per eseguire cose:

from importer.yahoo_json_data_importer import YahooJSONDataImporter, DatabaseAdapter
from pathlib import Path

if __name__ == '__main__':
    database_adapter = DatabaseAdapter()
    file_data_importer = YahooJSONDataImporter(database_adapter)
    file_data_importer.from_file(Path("data/YahooFinance_A_27-03-20_to_20-03-20.json"))