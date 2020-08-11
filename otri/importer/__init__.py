from ..database.database_adapter import DatabaseAdapter
from typing import Mapping, Sequence
import json
from pathlib import Path
from ..utils import logger as log


class DataImporter:
    '''
    Abstract class, used to import data from a correctly formatted stream to a
    database of any kind (MongoDB, DynamoDB, Postrgres JSON, etc).

    Attributes:
        database : DatabaseAdapter
            Adapter for whatever database it'll be using to store given data.
    '''

    def __init__(self, database: DatabaseAdapter):
        '''
        Constructor method, requires database connection.

        Parameters:
            database : DatabaseAdapter
                Adapter for the database where to store the imported data
        '''
        self.database = database

    def from_contents(self, contents: Mapping[Mapping, Sequence[Mapping]]):
        '''
        Imports data given a pre-formatted content.

        Parameters:
            contents : dict
                The contents of pre-formatted data downloaded using a downloader.
        '''
        pass

    def from_json_file(self, json_file_path: Path):
        '''
        Imports data given a json file path.

        Parameters:
            json_file_path : pathlib.Path
                The path of the json file to import.
        '''
        with json_file_path.open() as json_file:
            try:
                json_file_contents = json.load(json_file)
            except (Exception) as error:
                log.e("Unable to load file {}: {}".format(json_file_path, error))
        self.from_contents(json_file_contents)
