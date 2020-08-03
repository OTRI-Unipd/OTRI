"""
Module used to update JSON data to the metadata table in the database.
"""

__autor__ = "Luca Crema <lc.crema@hotmail.com>"
__version__ = "0.1"

import json
from pathlib import Path
from otri.utils import config, logger as log
import psycopg2
from psycopg2.extras import execute_values
import collections
from typing import Sequence

TICKER_LISTS_FOLDER = Path("docs/")

def list_docs_file(ticker_list_folder: Path) -> Path:
    '''
    List json files from the docs folder.

    Returns:
        List of names of json files without their extension.
    '''
    docs_glob = ticker_list_folder.glob('*.json')
    return [x.name.replace('.json', '') for x in docs_glob if x.is_file()]

def find_duplicates(ticker_list : Sequence[tuple]) -> Sequence[str]:
    '''
    Finds duplicate ticker names.
    '''
    counter = dict()
    duplicates = list()
    for atom_tuple in ticker_list:
        atom = json.loads(atom_tuple[0])
        counter[atom['ticker']] = counter.get(atom['ticker'], 0) + 1
        if counter[atom['ticker']] == 2: # Don't want to count it more times
            duplicates.append(atom['ticker'])
    return duplicates

if __name__ == "__main__":
    # Setup database connection
    try:
        log.i("Trying to connect to PGSQL Database")
        db_connection = psycopg2.connect(
            user=config.get_value("postgre_username"),
            password=config.get_value("postgre_password"),
            host=config.get_value("postgre_host"),
            port=config.get_value("postgre_port","5432"))
        cursor = db_connection.cursor()
        log.i("Connected to PGSQL")

    except (Exception, psycopg2.Error) as error:
        log.e("Error while connecting to PostgreSQL: {}".format(error))
        quit(-1)
    
    # Choose metadata file
    chosen_meta_file = None
    meta_files = list_docs_file(TICKER_LISTS_FOLDER)
    while chosen_meta_file == None:
        input_name = input("Choose a metadata file {} ".format(meta_files))
        if input_name in meta_files:
            chosen_meta_file = Path(TICKER_LISTS_FOLDER, input_name + ".json")
        else:
            log.w("{} not in possible files".format(input_name))

    # Choose if override
    override = input("Override data? y/n ").lower() == "y"

    # Open file and load atoms
    doc = json.load(chosen_meta_file.open("r"))
    data_json_list = [(json.dumps(atom),) for atom in doc['tickers']]

    # Find duplicates
    duplicates = find_duplicates(data_json_list)
    if len(duplicates) > 0:
        log.i("you have to remove the following duplicates before proceding: {}".format(duplicates))
        quit()
    
    log.i("beginning upload")

    # Upload actual metadata
    execute_values(cursor,"INSERT INTO metadata (data_json) VALUES %s ON CONFLICT (lower(data_json->>'ticker'::text)) DO UPDATE SET data_json = jsonb_recursive_merge(metadata.data_json, EXCLUDED.data_json, {})".format('true' if override else 'false'), data_json_list)
    db_connection.commit()

    log.i("upload completed")
