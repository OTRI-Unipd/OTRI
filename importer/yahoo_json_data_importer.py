from importer.json_data_importer import JSONDataImporter, DatabaseAdapter
from database.database_data import DatabaseData
from datetime import datetime
import json

class YahooJSONDataImporter(JSONDataImporter):
    '''
    Imports Yahoo historycal data into the database

    Attributes:
        database : DatabaseAdapter
            Database where to store data
    '''

    def from_contents(self, json_file_contents : dict):
        '''
        Imports data given a json document content.

        Parameters:
            json_file_contents : dict
                A dictionary of key-values, should be loaded using json.load(filepath) from a file or DataFrame.__to_json(orient="table", indent=4)
        '''
        new_data = self.__to_lowercase_keys(json_file_contents)
        atoms = self.__add_metadata_to_atoms(metadata=new_data['metadata'], atoms=new_data['data'])
        atoms = self.__fix_atoms_datetime(atoms)
        self.database.write(DatabaseData("atoms_j",atoms))

    def __fix_atoms_datetime(self, atoms : list):
        for atom in atoms:
            atom['datetime'] = datetime.strptime(atom['datetime'],"%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        return atoms

    def __add_metadata_to_atoms(self, metadata : dict, atoms : list):
        '''
        Adds "source" : source to all dictionary elements of the given list

        Parameters:
            source : str
                Name of the source where the data has been imported from
            atoms : list
                List of atoms to import
        '''
        for atom in atoms:
            for key,value in metadata.items():
                atom[key] = value
        return atoms

    def __to_lowercase_keys(self, data_dict : dict):
        '''
        Renames all dictionary's keys to lowercase

        Parameters:
            data_dict : dict
                Dictionary to edit
        Returns:
            dict containing all keys of the given dictionary lower cased
        '''
        new_data_dict = dict()
        for key,value in data_dict.items():
            lower_key = key.lower()
            if(type(value) == dict):
                new_data_dict[lower_key] = self.__to_lowercase_keys(value)
            if(type(value) == list):
                new_data_dict[lower_key] = list()
                for element in value:
                    if(type(element) == dict):
                        new_data_dict[lower_key].append(self.__to_lowercase_keys(element))
                    else:
                        print(element)
                        new_data_dict[lower_key].append(element)
            else:
                new_data_dict[lower_key] = value
        return new_data_dict
