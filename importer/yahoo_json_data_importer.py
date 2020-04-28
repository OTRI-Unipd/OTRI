from importer.json_data_importer import JSONDataImporter, DatabaseAdapter, METADATA_ATOM_KEYS
from downloader.timeseries_downloader import ATOMS_KEY, METADATA_KEY
from database.database_data import DatabaseData
from datetime import datetime
import importer.json_key_handler as json_kh
import json

class YahooJSONDataImporter(JSONDataImporter):
    '''
    Imports Yahoo historical data into the database.
    '''

    def from_contents(self, json_file_contents : dict):
        '''
        Imports data given a json document content.

        Parameters:
            json_file_contents : dict
                A dictionary of key-values, should be loaded using json.load(filepath) from a file or DataFrame.__to_json(orient="table", indent=4)
        '''
        atoms = YahooJSONDataImporter.__prepare_atoms(json_file_contents)
        self.database.write(DatabaseData("atoms_b",atoms))

    @staticmethod
    def __prepare_atoms(data : dict):
        '''
        Prepares atoms to be ready to be written into DB.

        Parameters:
            data : dict
                JSON document containing atoms and metadata.
                Atoms must be contained in data[timeseries_downloader.ATOMS_KEY].
                Metadata must be contained in data[timeseries_downloader.METADATA_KEY]
        Returns:
            list of atoms ready for DB with metadata and fixed date format.
        '''
        for atom in data[ATOMS_KEY]:
            YahooJSONDataImporter.__add_metadata_to_atom(data[METADATA_KEY], atom)
        return data[ATOMS_KEY]

    @staticmethod
    def __add_metadata_to_atom(metadata : dict, atom : dict):
        '''
        Adds every entry of metadata to the atom.

        Parameters:
            metadata : dict
                All kind of metadata to add to the atom.
            atom : dict
                Atom from the list of atoms.
        '''
        for key in METADATA_ATOM_KEYS:
            atom[key] = metadata[key]
        return atom
