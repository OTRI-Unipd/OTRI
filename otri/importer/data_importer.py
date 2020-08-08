
from ..database.database_adapter import DatabaseAdapter
from ..downloader.timeseries_downloader import ATOMS_KEY, METADATA_KEY
from typing import Mapping, Sequence
from ..utils import logger as log
from pathlib import Path
import json

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
    Atom-izes time series data by appending all metadata fields to every atom.
    '''

    def from_contents(self, contents: Mapping[Mapping, Sequence[Mapping]], database_table: str = "atoms_b"):
        '''
        Imports data given a pre-formatted content.

        Parameters:
            contents : dict\n
                The contents of pre-formatted data downloaded using a downloader.\n
            database_table : str\n
                Table name, the table is expected to contain at least a "data_json" JSON or JSONB
                type column, and any other optional column.
        '''
        atoms_table = self.database.get_tables()[database_table]
        atoms = DefaultDataImporter.__prepare_atoms(contents, atoms_table)
        self.database.add_all(atoms)

    @staticmethod
    def __prepare_atoms(contents: Mapping[Mapping, Sequence[Mapping]], table) -> Sequence[Mapping]:
        '''
        Appends each metadata field to all atoms of the given file contents.

        Parameters:
            contents : Mapping["metadata":Mapping,"atoms":Sequence[Mapping]]\n
                The un-formatted data to convert into atoms.\n
            table\n
                The table class to use when building the atoms. Must have "data_json" as the only
                mandatory field.
        Returns:
            List of atoms with metadata attached, converted to objects.
        '''
        atom_list = list()
        for atom in contents[ATOMS_KEY]:
            # Insert metadata
            for key in contents[METADATA_KEY]:
                atom[key] = contents[METADATA_KEY][key]
            # Convert to table object
            atom_list.append(table(data_json=atom))
        return atom_list
