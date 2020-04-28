from importer.data_importer import DataImporter, DatabaseAdapter, METADATA_ATOM_KEYS
from pathlib import Path
import json

class JSONDataImporter(DataImporter):
    '''
    Abstract class used to import financial data from json files into database.

    Attributes:
        database : DatabaseAdapter
            Adapter for whatever database we'll be using
    '''

    def from_contents(self, json_file_contents : dict):
        '''
        Imports data given a json document content.

        Parameters:
            json_file_contents : dict
                A dictionary of key-values, should be loaded using json.load(filepath) from a file or DataFrame.__to_json(orient="table", indent=4)
        Raises:
            NotImplementedError
                This method should be called on a sub-class that implements it 
        '''
        raise NotImplementedError

    def from_file(self, json_file_path : Path):
        '''
        Imports data given a json file path.

        Parameters:
            json_file_path : pathlib.Path
                The path of the file to import.
        '''
        json_file_contents = self.__import_file_contents(json_file_path)
        self.from_contents(json_file_contents)

    def __import_file_contents(self, json_file_path : Path):
        '''
        Loads the json file from a given path

        Parameters:
            json_file_path : Path
                The path of a JSON file containing the data
        Returns: 
            A dictionary of key-values from the loaded JSON file
        '''
        with json_file_path.open() as json_file:
            try:
                json_file_contents = json.load(json_file)
            except (Exception) as error:
                print("Unable to work on file {}: {}".format(json_file_path, error))
            #print(json_file_contents)
        return json_file_contents