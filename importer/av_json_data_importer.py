from importer.json_data_importer import JSONDataImporter, DatabaseAdapter, METADATA_ATOM_KEYS
from downloader.timeseries_downloader import METADATA_KEY, ATOMS_KEY
from database.database_data import DatabaseData
from datetime import datetime
import importer.json_key_handler as json_kh
import json

class AVJSONDataImporter(JSONDataImporter):
    '''
    Imports Alpha Vantage historycal data into the database
    '''

    def from_contents(self, json_file_contents: dict):
        '''
        Imports data given a json document content.

        Parameters:
            json_file_contents : dict
                A dictionary of key-values, should be loaded using json.load(filepath) from a file or DataFrame.__to_json(orient="table", indent=4).
        '''
        atoms = self.__add_metadata_to_atoms(json_file_contents[METADATA_KEY], json_file_contents[ATOMS_KEY])
        self.database.write(DatabaseData("atoms_b", atoms))

    def __add_metadata_to_atoms(self, metadata: dict, atoms: list):
        '''
        Adds each metadata field to all atoms of the given list

        Parameters:
            source : str
                Name of the source where the data has been imported from
            atoms : list
                List of atoms to import
        '''
        for atom in atoms:
            for key in METADATA_ATOM_KEYS:
                atom[key] = metadata[key]
        return atoms
