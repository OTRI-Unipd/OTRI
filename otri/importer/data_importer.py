
from ..database.database_adapter import DatabaseAdapter, DatabaseData
from ..downloader.timeseries_downloader import META_INTERVAL_KEY, META_PROVIDER_KEY, META_TICKER_KEY, ATOMS_KEY, METADATA_KEY
from typing import Mapping, Sequence
from ..utils import logger as log
from pathlib import Path
import json

'''
Keys to grab from metadata and append to every atom
'''
METADATA_ATOM_KEYS = [META_INTERVAL_KEY, META_PROVIDER_KEY, META_TICKER_KEY]


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

    def from_json_file(self, json_file_path : Path):
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


class DefaultDataImporter(DataImporter):
    '''
    Atom-izes time series data by appending metadata fields to every atom.
    '''

    def from_contents(self, contents: Mapping[Mapping, Sequence[Mapping]], database_table: str = "atoms_b"):
        '''
        Imports data given a pre-formatted content.

        Parameters:
            contents : dict
                The contents of pre-formatted data downloaded using a downloader.
        '''
        atoms = DefaultDataImporter.__prepare_atoms(contents)
        self.database.write(DatabaseData(database_table, atoms))

    @staticmethod
    def __prepare_atoms(contents: Mapping[Mapping, Sequence[Mapping]]) -> Sequence[Mapping]:
        '''
        Appends each metadata field to all atoms of the given file contents.

        Parameters:
            contents : Mapping["metadata":Mapping,"atoms":Sequence[Mapping]]
        Returns:
            List of atoms with metadata attached.
        '''
        for atom in contents[ATOMS_KEY]:
            for key in METADATA_ATOM_KEYS:
                atom[key] = contents[METADATA_KEY][key]
        return contents[ATOMS_KEY]
