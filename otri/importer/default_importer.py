from ..downloader import ATOMS_KEY, METADATA_KEY
from typing import Mapping, Sequence
from . import DataImporter


class DefaultImporter(DataImporter):
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
        atoms = DefaultImporter.__prepare_atoms(contents, atoms_table)
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
