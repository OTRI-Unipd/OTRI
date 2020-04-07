from importer.json_data_importer import JSONDataImporter, DatabaseAdapter
from database.database_data import DatabaseData
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
        atoms = json_file_contents['data']
        self.__to_lowercase_atoms(atoms)
        #self.database.write(DatabaseData(""))

    def __to_lowercase_atoms(self, data_list : list):
        new_data_list = list()
        for atom in data_list:
            new_atom = dict()
            for key in atom.keys():
                new_atom[key.lower()] = atom[key]
            new_data_list.append(new_atom)
        return new_data_list
