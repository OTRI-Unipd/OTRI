'''
Module that can be run to upload new metadata.\n
If -p is passed it queries the provider for availability.\n

Usage:\n
python metadata_upload.py [-f <FILE>] [-p <PROVIDER>] [--override] [-c --console-input]
'''

__autor__ = "Luca Crema <lc.crema@hotmail.com>"
__version__ = "1.0"

import json
from pathlib import Path

from sqlalchemy import func

from otri.database.postgresql_adapter import PostgreSQLAdapter, DatabaseAdapter
from otri.downloader.tradier import TradierMetadata
from otri.downloader.yahoo_downloader import YahooMetadata
from otri.utils import config
from otri.utils import logger as log
from otri.utils.cli import CLI, CLIFlagOpt, CLIValueOpt
from sqlalchemy.exc import IntegrityError

PROVIDERS = {
    "YahooFinance": {"class": YahooMetadata, "args": {}},
    "Tradier": {"class": TradierMetadata, "args": {"key": config.get_value("tradier_api_key")}}
}
FILE_DIRECTORY = Path('docs/')
JSON_FILES = [x.name.replace('.json', '') for x in FILE_DIRECTORY.glob('*.json') if x.is_file()]


def upload_with_provider(provider, db_adapter: DatabaseAdapter, metadata_table, atom: dict, override: bool):
    '''
    Uploads one piece of metadata at a time updating metadata using the provider.
    '''
    info = source.info([atom['ticker']])
    if info is False or len(info) == 0:
        log.i("{} not supported".format(atom['ticker']))
    else:
        # Extend the atom with new metadata (that will probably override old metadata)
        log.i("Extended {} metadata with provider data: {}".format(atom['ticker'], info[0]))
        atom.update(info[0])
    # Upload data in DB
    upload_data(db_adapter=db_adapter, metadata_table=metadata_table, atom=atom, override=override)


def upload_data(db_adapter: DatabaseAdapter, metadata_table, atom: dict, override: bool):
    '''
    Uploads an atom of metadata in the db.
    '''
    log.d("uploading {} metadata to db".format(atom['ticker']))
    try:
        with db_adapter.begin() as conn:
            insert_query = metadata_table.insert().values(data_json=atom)
            conn.execute(insert_query)
    except IntegrityError:
        log.w("ticker {} alrady in DB, updating its metadata".format(atom['ticker']))
        with db_adapter.begin() as conn:
            update_query = metadata_table.update().values(data_json=func.jsonb_recursive_merge(metadata_table.c.data_json, json.dumps(atom), override))\
                .where(metadata_table.c.data_json["ticker"].astext == atom['ticker'])
            conn.execute(update_query)
    log.d("upload {} completed".format(atom['ticker']))


if __name__ == "__main__":

    cli = CLI(name="metadata_update",
              description="Script that downloads weekly historical timeseries data.",
              options=[
                  CLIValueOpt(
                      short_name="p",
                      long_name="provider",
                      short_desc="Provider",
                      long_desc="Provider for the historical data.",
                      required=False,
                      values=list(PROVIDERS.keys())
                  ),
                  CLIValueOpt(
                      short_name="f",
                      long_name="file",
                      short_desc="File",
                      long_desc="File containing metatdata.",
                      required=False,
                      values=list(JSON_FILES)
                  ),
                  CLIFlagOpt(
                      long_name="override",
                      short_desc="Override DB data",
                      long_desc="If a duplicate key is found the DB data will be overridden by new downloaded data."
                  ),
                  CLIFlagOpt(
                      short_name="c",
                      long_name="console-input",
                      short_desc="Use console input",
                      long_desc="Uses console input as metadata source. If -f is provided it will upload the file first."
                  )
              ])
    values = cli.parse()
    provider = values['-p']
    meta_file = values['-f']
    override = values['--override']
    console_input = values['-c']

    # Setup database connection
    db_adapter = PostgreSQLAdapter(
        host=config.get_value("postgresql_host"),
        port=config.get_value("postgresql_port", "5432"),
        user=config.get_value("postgresql_username", "postgres"),
        password=config.get_value("postgresql_password"),
        database=config.get_value("postgresql_database", "postgres")
    )
    metadata_table = db_adapter.get_tables()['metadata']

    # Setup provider if required
    if provider is not None:
        log.i("setting up {}".format(provider))
        args = PROVIDERS[provider]["args"]
        source = PROVIDERS[provider]["class"](**args)

    # Upload meta file if required
    if(meta_file is not None):
        log.i("uploading {} metadata contents".format(meta_file))
        meta_file_dict = json.load(Path(FILE_DIRECTORY, meta_file + ".json").open("r"))
        file_atoms = meta_file_dict['tickers']
        if provider is not None:
            for atom in file_atoms:
                upload_with_provider(source, db_adapter, metadata_table, atom, override)
        else:
            for atom in file_atoms:
                upload_data(db_adapter, metadata_table, atom, override)
                # Read console data if required

    db_adapter.close()
