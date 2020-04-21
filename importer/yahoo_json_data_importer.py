from importer.json_data_importer import JSONDataImporter, DatabaseAdapter
from database.database_data import DatabaseData
from datetime import datetime
import importer.json_key_handler as json_kh
import json

class YahooJSONDataImporter(JSONDataImporter):
    '''
    Imports Yahoo historycal data into the database
    '''

    def from_contents(self, json_file_contents : dict):
        '''
        Imports data given a json document content.

        Parameters:
            json_file_contents : dict
                A dictionary of key-values, should be loaded using json.load(filepath) from a file or DataFrame.__to_json(orient="table", indent=4)
        '''
        new_data = self.__to_lowercase_keys(json_file_contents)
        atoms = self.__prepare_data(new_data)
        self.database.write(DatabaseData("atoms_b",atoms))

    def __prepare_data(self, data : dict):
        '''
        Prepares atoms to be ready to be written into DB.

        Parameters:
            data : dict
                JSON document containing atoms and metadata.
                Atoms must be contained in data['data'].
                Metadata must be contained in data['metadata']
        Returns:
            list of atoms ready for DB with metadata and fixed date format.
        '''
        for atom in data['data']:
            self.__add_metadata_to_atom(data['metadata'], atom)
            self.__fix_atom_datetime(atom)
        return data['data']

    def __fix_atom_datetime(self, atom : dict):
        '''
        Changes yahoo UTC datetime to a 'Y-m-d H:m:s.ms' format

        Parameters:
            atom : dict
                An atom from the list of atoms.
        Returns:
            The atom with the modified date.
        '''
        atom['datetime'] = datetime.strptime(atom['datetime'],"%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        return atom

    def __add_metadata_to_atom(self, metadata : dict, atom : dict):
        '''
        Adds every entry of metadata to the atom.

        Parameters:
            metadata : dict
                All kind of metadata to add to the atom.
            atom : dict
                Atom from the list of atoms.
        '''
        for key,value in metadata.items():
            atom[key] = value
        return atom

    def __to_lowercase_keys(self, data_dict : dict):
        '''
        Renames all dictionary's keys to lowercase

        Parameters:
            data_dict : dict
                Dictionary to edit
        Returns:
            dict containing all keys of the given dictionary lower cased
        '''
        return json_kh.lower_all_keys_deep(data_dict)
