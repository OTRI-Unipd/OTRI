'''
Module that can be cron-job'd used to update atoms metadata retrieved from multiple sources.
'''

__autor__ = "Luca Crema <lc.crema@hotmail.com>"
__version__ = "1.0"

import getopt
import json
import sys

from sqlalchemy import func

from otri.database.postgresql_adapter import PostgreSQLAdapter
from otri.downloader.yahoo_downloader import YahooMetadata
from otri.utils import config
from otri.utils import logger as log

SOURCES = {
    "YahooFinance": {"class": YahooMetadata, "args": {}}
}


def print_error_msg(msg: str = None):
    if not msg is None:
        msg = msg + ": "

    log.e("{}metadata_cli_update.py -p <provider: {}> -o <override db values: [y/n]>".format(
        msg,
        list(SOURCES.keys())
    )
    )


if __name__ == "__main__":

    if len(sys.argv) < 1:
        print_error_msg("Not enough arguments")
        quit(2)

    provider = ""
    override = False
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hp:o:", ["help", "provider=", "override="])
    except getopt.GetoptError as e:
        # If the passed option is not in the list it throws error
        print_error_msg(str(e))
        quit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print_error_msg()
            quit()
        elif opt in ("-o", "--override"):
            override = arg == "y"
        elif opt in ("-p", "--provider"):
            provider = arg

    # Check if necessary arguments have been given
    if provider == "":
        print_error_msg("Missing argument provider")
        quit(2)

    # Check if passed arguments are valid
    if not provider in list(SOURCES.keys()):
        print_error_msg("Provider {} not supported".format(provider))
        quit(2)

    # Retrieve provider object
    args = SOURCES[provider]["args"]
    source = SOURCES[provider]["class"](**args)

    # Setup database connection
    db_adapter = PostgreSQLAdapter(
        host=config.get_value("postgresql_host"),
        port=config.get_value("postgresql_port", "5432"),
        user=config.get_value("postgresql_username", "postgres"),
        password=config.get_value("postgresql_password"),
        database=config.get_value("postgresql_database", "postgres")
    )
    metadata_table = db_adapter.get_tables()['metadata']

    # Load ticker list
    log.d("loading ticker list from db")
    with db_adapter.begin() as conn:
        query = metadata_table.select()\
            .where(metadata_table.c.data_json.has_key('ticker'))\
            .order_by(metadata_table.c.data_json['ticker'].astext)
        result = conn.execute(query)
    tickers = [row[1]['ticker'] for row in result.fetchall()]
    log.d("successfully read ticker list")

    # Start metadata download and upload
    log.i("beginning metadata retrieval with provider {} and override {}".format(provider, override))
    for ticker in tickers:
        log.i("working on {}".format(ticker))
        info = source.get_info(ticker)
        if info is False:
            log.i("{} not supported by {}".format(ticker, provider))
            continue
        sql_override = 'true' if override else 'false'
        log.d("uploading {} metadata to db".format(ticker))
        with db_adapter.begin() as conn:
            old = db_adapter.get_tables()['metadata']
            query = metadata_table.update().values(data_json=func.jsonb_recursive_merge(old.c.data_json, json.dumps(info), override))\
                .where(metadata_table.c.data_json["ticker"].astext == ticker)
            conn.execute(query)
        log.d("upload {} completed".format(ticker))
