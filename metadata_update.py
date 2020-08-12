'''
Module that can be cron-job'd used to update atoms metadata retrieved from multiple sources.
'''

__autor__ = "Luca Crema <lc.crema@hotmail.com>"
__version__ = "1.1"

import json

import psycopg2

from otri.database.postgresql_adapter import PostgreSQLAdapter
from otri.downloader.yahoo_downloader import YahooMetadata
from otri.downloader.tradier import TradierMetadata
from otri.utils import config
from otri.utils import logger as log
from otri.utils.cli import CLI, CLIValueOpt, CLIFlagOpt

PROVIDERS = {
    "YahooFinance": {"class": YahooMetadata, "args": {}},
    "Tradier": {"class": TradierMetadata, "args": {"key": config.get_value("tradier_api_key")}}
}


if __name__ == "__main__":

    cli = CLI(name="timeseries_cli_dw",
              description="Script that downloads weekly historical timeseries data.",
              options=[
                  CLIValueOpt(
                      short_name="p",
                      long_name="provider",
                      short_desc="Provider",
                      long_desc="Provider for the historical data.",
                      required=True,
                      values=list(PROVIDERS.keys())
                  ),
                  CLIFlagOpt(
                      long_name="override",
                      short_desc="Override DB data",
                      long_desc="If a duplicate key is found the DB data will be overridden by new downloaded data."
                  )
              ])

    values = cli.parse()
    provider = values['-p']
    override = values['--override']

    # Retrieve provider object
    args = PROVIDERS[provider]["args"]
    source = PROVIDERS[provider]["class"](**args)

    # Setup database connection
    try:
        log.d("trying to connect to PGSQL Database")
        db_connection = PostgreSQLAdapter(
            host=config.get_value("postgresql_host"),
            port=config.get_value("postgresql_port", "5432"),
            user=config.get_value("postgresql_username", "postgres"),
            password=config.get_value("postgresql_password"),
            database=config.get_value("postgresql_database", "postgres")
        )
        log.d("connected to PGSQL")
    except (Exception, psycopg2.Error) as error:
        log.e("Error while connecting to PostgreSQL: {}".format(error))
        quit(-1)

    # Load ticker list
    log.d("loading ticker list from db")
    result = db_connection._engine.connect().execute("SELECT data_json->>'ticker' FROM metadata WHERE data_json?'ticker' ORDER BY data_json->>'ticker';")
    tickers = [row[0] for row in result]
    log.d("successfully read ticker list")

    # Start metadata download and upload
    log.i("beginning metadata retrieval with provider {} and override {}".format(provider, override))
    for ticker in tickers:
        log.i("working on {}".format(ticker))
        info = source.info([ticker])
        print("Info: {}".format(info))
        if info is False:
            log.i("{} not supported by {}".format(ticker, provider))
            continue
        info = info[0] # TODO: handle multiple tickers info at once
        sql_override = 'true' if override else 'false'
        log.d("uploading {} metadata to db".format(ticker))
        db_connection._engine.connect().execute("UPDATE metadata AS m SET data_json = jsonb_recursive_merge(old.data_json, %s, {}) FROM metadata AS old WHERE m.data_json->>'ticker' = '{}' AND m.id = old.id".format(
            sql_override, ticker), (json.dumps(info),))
        log.d("upload {} completed".format(ticker))
