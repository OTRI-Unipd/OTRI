from ..downloader import ATOMS_KEY, METADATA_KEY
from typing import Mapping, Sequence
from . import DataImporter


class DefaultDataImporter(DataImporter):
    '''
    Atom-izes time series data by appending all metadata fields to every atom.
    '''

    def from_contents(self, contents: Mapping[Mapping, Sequence[Mapping]], database_table: str = "atoms_b",
                      json_column: str = "data_json"):
        '''
        Imports data given a pre-formatted content.

        Parameters:
            contents : dict
                The contents of pre-formatted data downloaded using a downloader.\n
            database_table : str
                Table name, the table is expected to contain at least a "data_json" JSON or JSONB
                type column, and any other optional column.\n
            json_column : str
                Column name for the JSON data. Defaults to "data_json". Must be in the given table.
        '''
        atoms = DefaultDataImporter.__prepare_atoms(contents, json_column)
        self.database.insert(database_table, atoms)

    @staticmethod
    def __prepare_atoms(contents: Mapping[Mapping, Sequence[Mapping]], json_column: str) -> Sequence[Mapping]:
        '''
        Appends each metadata field to all atoms of the given file contents.

        Parameters:
            contents : Mapping["metadata":Mapping,"atoms":Sequence[Mapping]]
                The un-formatted data to convert into atoms.\n
            json_column : str
                The column for the JSON data.\n
        Returns:
            List of atoms with metadata attached, converted to objects.
        '''
        atom_list = list()
        for atom in contents[ATOMS_KEY]:
            # Insert metadata
            for key in contents[METADATA_KEY]:
                atom[key] = contents[METADATA_KEY][key]
            # Convert to table object
            atom_list.append({json_column: atom})
        return atom_list
