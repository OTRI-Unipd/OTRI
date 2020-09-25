'''
Updates the metadata table in the database.
'''

__author__ = "Luca Crema <lc.crema@hotmail.com>"
__version__ = "0.1"

import json

from otri.database.postgresql_adapter import PostgreSQLAdapter
from otri.utils import config
from otri.utils import logger as log
from sqlalchemy.exc import IntegrityError

ATOMS_TABLE = "atoms_b"
METADATA_TABLE = "metadata"
METADATA_TYPE = "metadata"

def rec_merge(a1 : dict, a2 : dict, override : bool = False) -> dict:
    '''
    Merges recursively two dictionaries extending lists and inner dictionaries.\n

    Parameters:\n
        a1, a2 : dict
            Two dictionaries.
        override : bool
            If the same key is in both dictionaries and override is True the outputted dictionary will have the second dictionary's value.
    '''
    for key, value in a2.items():
        if value is None:
            continue
        if key not in a1:
            a1[key] = value
        else:
            if isinstance(a1[key], list) and isinstance(value, list):
                # Avoid duplicates
                a1[key].extend([x for x in value if x not in a1[key]])
                continue
            elif isinstance(a1[key], dict) and isinstance(value, dict):
                a1[key] = rec_merge(a1[key], value)
                continue
            # Override
            elif override:
                a1[key] = a2[key]
    return a1

if __name__ == "__main__":

     # Setup database connection
    db_adapter = PostgreSQLAdapter(
        host=config.get_value("postgresql_host"),
        port=config.get_value("postgresql_port", "5432"),
        user=config.get_value("postgresql_username", "postgres"),
        password=config.get_value("postgresql_password"),
        database=config.get_value("postgresql_database", "postgres")
    )
    atoms_table = db_adapter.get_tables()[ATOMS_TABLE]
    metadata_table = db_adapter.get_tables()[METADATA_TABLE]

    with db_adapter.begin() as conn:
        query = atoms_table.select()\
            .where(atoms_table.c.data_json['type'].astext == METADATA_TABLE)\
            .where(atoms_table.c.data_json.has_key('ticker'))\
            .order_by(atoms_table.c.data_json['ticker'].astext)
        result = conn.execute(query)

    if result == None:
        log.e("unable to load metadata atoms")
        quit(1)
    
    metadata_atoms = result.fetchall()

    if not metadata_atoms:
        log.w("unable to find metadata atoms in {} table".format(ATOMS_TABLE))
        quit(1)
    
    tickers_atoms = dict()
    for atom in metadata_atoms:
        ticker = atom['data_json']['ticker']
        if ticker not in tickers_atoms:
            tickers_atoms[ticker] = list()
        tickers_atoms[ticker].append(atom['data_json'])
    
    for ticker in tickers_atoms.keys():
        # Merge atoms of the same ticker
        processed_metadata = dict()
        for atom in tickers_atoms[ticker]:
            processed_metadata = rec_merge(processed_metadata, atom, override=True)
        # Insert atoms in metadata table. On duplicate override.
        log.i("inserting {} in metadata table".format(processed_metadata['ticker']))
        try:
            db_adapter.insert(METADATA_TABLE, {'data_json':processed_metadata})
        except IntegrityError:
            log.i("duplicate metadata, overriding")
            with db_adapter.begin() as conn:
                del_query = metadata_table.update().where(metadata_table.c.data_json['ticker'].astext == processed_metadata['ticker']).values(data_json=processed_metadata)
                conn.execute(del_query)
            log.i("new data inserted")


