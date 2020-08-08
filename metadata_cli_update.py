"""
Module that can be cron-job'd used to update atoms metadata retrieved from multiple sources.
"""

__autor__ = "Luca Crema <lc.crema@hotmail.com>"
__version__ = "1.0"

import getopt
import json
import sys

import psycopg2
from psycopg2.extras import execute_values

from otri.downloader.yahoo_downloader import YahooMetadataDW
from otri.utils import config
from otri.utils import logger as log

SOURCES = {
    "YahooFinance": {"class": YahooMetadataDW, "args":{}} 
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
    try:
        log.d("trying to connect to PGSQL Database")
        db_connection = psycopg2.connect(
            user=config.get_value("postgre_username"),
            password=config.get_value("postgre_password"),
            host=config.get_value("postgre_host"),
            port=config.get_value("postgre_port","5432"))
        cursor = db_connection.cursor()
        log.d("connected to PGSQL")
    except (Exception, psycopg2.Error) as error:
        log.e("Error while connecting to PostgreSQL: {}".format(error))
        quit(-1)
    
    # Load ticker list
    log.d("loading ticker list from db")
    cursor.execute("SELECT data_json->>'ticker' FROM metadata WHERE data_json?'ticker' ORDER BY data_json->>'ticker';")
    tickers = [row[0] for row in cursor.fetchall()]
    log.d("successfully read ticker list")

    # Start metadata download and upload
    log.i("beginning metadata retrieval with provider {} and override {}".format(provider, override))
    for ticker in tickers:
        log.i("working on {}".format(ticker))
        info = source.get_info(ticker)
        if info == False:
            log.i("{} not supported by {}".format(ticker, provider))
            continue
        sql_override = 'true' if override else 'false'
        log.d("uploading {} metadata to db".format(ticker))
        cursor.execute("UPDATE metadata AS m SET data_json = jsonb_recursive_merge(old.data_json, %s, {}) FROM metadata AS old WHERE m.data_json->>'ticker' = '{}' AND m.id = old.id".format(sql_override, ticker),(json.dumps(info),))
        db_connection.commit()
        log.d("upload {} completed".format(ticker))

    # Close DB connection
    cursor.close()
    db_connection.close()
