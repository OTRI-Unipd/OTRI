from importer.json_data_importer import JSONDataImporter, DatabaseAdapter
from database.database_data import DatabaseData
from datetime import datetime
import importer.json_key_handler as json_kh
import json

AV_ALIASES = {
    "1. open": "open",
    "2. high": "high",
    "3. low": "low",
    "4. close": "close",
    "5. volume": "volume"
}

METADATA_KEY = "metadata"
DATETIME_KEY = "datetime"


class AVJSONDataImporter(JSONDataImporter):
    '''
    Imports Alpha Vantage historycal data into the database
    '''

    def from_contents(self, json_file_contents: dict):
        '''
        Imports data given a json document content.

        Parameters:
            json_file_contents : dict
                A dictionary of key-values, should be loaded using json.load(filepath) from a file or DataFrame.__to_json(orient="table", indent=4)
                Expects a datetime key and a dict value.
        '''
        new_data = self.__rename_keys(json_file_contents)
        new_data = self.__add_timestamp(new_data)
        atoms = self._extract_atoms(new_data)
        atoms = self.__add_metadata_to_atoms(new_data[METADATA_KEY], atoms)
        atoms = self.__fix_atoms_datetime(atoms)
        self.database.write(DatabaseData("atoms_b", atoms))

    def __add_timestamp(self, data_dict: dict):
        '''
        Copies the timestamp key inside each atom as a dictionary entry.

        Parameters:
            data_dict : dict
                A dictionary of key-values pairs. Should be:
                {datetime : dict, datetime : dict, ...}
                For each dict item, will attempt to add the "datetime" field, using the item's key as the new field's value.
                Ignores item with the key METADATA_KEY, if any.
        Returns:
            The same dictionary it received, after modifying it.
        '''
        for key, value in data_dict.items():
            if key == METADATA_KEY or type(value) != dict:
                continue
            value[DATETIME_KEY] = key
        return data_dict

    def __extract_atoms(self, data_dict: dict):
        '''
        Takes the atoms list out of the data dictionary.

        Parameters:
            data_dict : dict
                A dictionary. Any fields with the METADATA_KEY will be ignored.
        Returns:
            The list of values in the dictionary except METADATA_KEY
        '''
        atoms = list(data_dict.values())
        if METADATA_KEY in data_dict.keys():
            atoms.remove(data_dict[METADATA_KEY])
        return atoms

    def __fix_atoms_datetime(self, atoms: list):
        for atom in atoms:
            atom[DATETIME_KEY] = datetime.strptime(
                atom[DATETIME_KEY], "%Y-%m-%d %H:%M:%S"
            ).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        return atoms

    def __add_metadata_to_atoms(self, metadata: dict, atoms: list):
        '''
        Adds each metadata field to all dictionary elements of the given list

        Parameters:
            source : str
                Name of the source where the data has been imported from
            atoms : list
                List of atoms to import
        '''
        for atom in atoms:
            for key, value in metadata.items():
                atom[key] = value
        return atoms

    def __rename_keys(self, data_dict: dict):
        '''
        Renames the dictionary's keys with some more fitting aliases.

        Parameters:
            data_dict : dict
                Dictionary to edit
        Returns:
            dict containing all keys of the given dictionary lower cased and with their names changed.
            See AV_ALIASES for details.
        '''
        return json_kh.rename_deep(data_dict, AV_ALIASES)
