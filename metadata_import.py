'''
Module that can be run to import new metadata.\n
If -p is passed it queries the provider for availability and other metadata.\n

Usage:\n
python metadata_import.py [-f <FILE>] [-p <PROVIDER>] [--override]
'''

__author__ = "Luca Crema <lc.crema@hotmail.com>"
__version__ = "1.0"

import json
from pathlib import Path

from otri.database.postgresql_adapter import DatabaseAdapter, PostgreSQLAdapter
from otri.downloader.tradier import TradierMetadata
from otri.downloader.yahoo import YahooMetadata
from otri.utils import config
from otri.utils import logger as log
from otri.utils.cli import CLI, CLIFlagOpt, CLIValueOpt

PROVIDERS = {
    "YahooFinance": {"class": YahooMetadata, "args": {}},
    "Tradier": {"class": TradierMetadata, "args": {"key": config.get_value("tradier_api_key")}}
}
FILE_DIRECTORY = Path('docs/')
JSON_FILES = [x.name.replace('.json', '') for x in FILE_DIRECTORY.glob('*.json') if x.is_file()]

ATOMS_TABLE = "atoms_b"


def upload_with_provider(provider, db_adapter: DatabaseAdapter, atoms_table, atom: dict, override: bool):
    '''
    Uploads one piece of metadata at a time updating metadata using the provider.
    '''
    info = source.info([atom['ticker']])
    if info is False or len(info) == 0:
        log.i("{} not supported by provider".format(atom['ticker']))
    else:
        # Extend the atom with new metadata (that will probably override old metadata)
        log.i("extended {} metadata with provider data".format(atom['ticker']))
        atom.update(info[0])
    # Upload data in DB
    upload_data(db_adapter=db_adapter, atoms_table=atoms_table, atom=atom, override=override)


def upload_data(db_adapter: DatabaseAdapter, atoms_table, atom: dict, override: bool):
    '''
    Uploads an atom of metadata in the db.
    '''
    log.d("uploading {} metadata to db".format(atom['ticker']))
    try:
        with db_adapter.begin() as conn:
            insert_query = atoms_table.insert().values(data_json=atom)
            conn.execute(insert_query)
    except Exception as e:
        log.w("there has been an exception while uploading data: {}".format(e))
    log.d("upload {} completed".format(atom['ticker']))


if __name__ == "__main__":

    cli = CLI(name="metadata_update",
              description="Uploads ticker lists and optionally updates their metadata using one provider.",
              options=[
                  CLIValueOpt(
                      short_name="f",
                      long_name="file",
                      short_desc="File",
                      long_desc="File containing metatdata.",
                      required=True,
                      values=list(JSON_FILES)
                  ),
                  CLIValueOpt(
                      short_name="p",
                      long_name="provider",
                      short_desc="Provider",
                      long_desc="Provider for the metadata.",
                      required=False,
                      values=list(PROVIDERS.keys())
                  ),
                  CLIFlagOpt(
                      long_name="override",
                      short_desc="Override DB data",
                      long_desc="If a duplicate key is found the DB data will be overridden by new data."
                  )
              ])
    values = cli.parse()
    provider = values['-p']
    meta_file = values['-f']
    override = values['--override']

    # Setup database connection
    db_adapter = PostgreSQLAdapter(
        host=config.get_value("postgresql_host"),
        port=config.get_value("postgresql_port", "5432"),
        user=config.get_value("postgresql_username", "postgres"),
        password=config.get_value("postgresql_password"),
        database=config.get_value("postgresql_database", "postgres")
    )
    atoms_table = db_adapter.get_tables()[ATOMS_TABLE]

    # Setup provider if required
    if provider is not None:
        log.i("setting up {}".format(provider))
        args = PROVIDERS[provider]["args"]
        source = PROVIDERS[provider]["class"](**args)

    # Upload meta file
    if(meta_file is not None):
        log.i("uploading {} metadata contents".format(meta_file))
        meta_file_dict = json.load(Path(FILE_DIRECTORY, meta_file + ".json").open("r"))
        file_atoms = meta_file_dict['tickers']
        if provider is not None:
            for atom in file_atoms:
                upload_with_provider(source, db_adapter, atoms_table, atom, override)
        else:
            for atom in file_atoms:
                upload_data(db_adapter, atoms_table, atom, override)
    else:
        log.d("metadata file not defined")
