from importer.data_importer import DataImporter, DatabaseAdapter
from pathlib import Path
import json

class JSONDataImporter(DataImporter):
    '''
    Abstract class used to import financial data from json files into database.

    Parameters:
        database : DatabaseAdapter
            Adapter for whatever database we'll be using

        json_file_path : Path
            Path for the JSON file to import
        json_file_content : dict
            Loaded JSON file contents
    '''
    def __init__(self, database : DatabaseAdapter, json_file_path : Path):
        '''
        Constructor method, requires the path of the file to import

        Arguments:
            json_file_path : Path
                Path for the JSON file to import
        '''
        super(JSONDataImporter,self).__init__(database)
        self.json_file_path = json_file_path


    def _import_file_contents(self):
        '''
        Loads the file and saves it as a dictionary into the json_file_content class parameter
        '''
        with self.json_file_path.open() as json_file:
            self.json_file_content = json.load(json_file)