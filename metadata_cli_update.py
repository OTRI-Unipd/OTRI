"""
Module that can be cron-job'd used to update atoms metadata retrieved from multiple sources.
"""
import getopt
import json
import sys

import psycopg2
from psycopg2.extras import execute_values

from otri.downloader.yahoo_downloader import YahooMetadataDW
from otri.utils import config
from otri.utils import logger as log

SOURCES = {
    "YahooFinance": YahooMetadataDW()
}

def print_error_msg(msg: str = None):
    if not msg is None:
        msg = msg + ": "
    
    log.e("{}metadata_cli_update.py -p <provider: {}> -o <override: [y/n]>".format(
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

    source = SOURCES[provider]

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
    cursor.execute("SELECT data_json as json FROM metadata ORDER BY data_json->>'ticker';")
    atoms = [row[0] for row in cursor.fetchall()]
    log.d("successfully read ticker list")

    # Start metadata download and upload
    log.i("beginning metadata retrieval with provider {} and override {}".format(provider, override))
    for atom in atoms:
        log.i("working on {}".format(atom['ticker']))
        info = source.get_info(atom['ticker'])
        if info == False:
            log.i("{} not supported by {}".format(atom['ticker'], provider))
            continue
        log.d("uploading {} metadata to db".format(atom['ticker']))
        cursor.execute("INSERT INTO metadata (data_json) VALUES (%s) ON CONFLICT (lower(data_json->>'ticker'::text)) DO UPDATE SET data_json = jsonb_recursive_merge(metadata.data_json, EXCLUDED.data_json, {})".format('true' if override else 'false'), (json.dumps(info),))
        db_connection.commit()
        log.d("upload {} completed".format(atom['ticker']))

    # Close DB connection
    cursor.close()
    db_connection.close()
