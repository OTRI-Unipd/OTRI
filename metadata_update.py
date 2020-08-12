'''
Module that can be cron-job'd used to update atoms metadata retrieved from multiple sources.

Usage:\n
python metadata_update.py -p <PROVIDER> [--override]
'''

__autor__ = "Luca Crema <lc.crema@hotmail.com>"
__version__ = "1.1"

import json

from sqlalchemy import func

from otri.database.postgresql_adapter import PostgreSQLAdapter
from otri.downloader.tradier import TradierMetadata
from otri.downloader.yahoo_downloader import YahooMetadata
from otri.utils import config
from otri.utils import logger as log
from otri.utils.cli import CLI, CLIFlagOpt, CLIValueOpt

PROVIDERS = {
    "YahooFinance": {"class": YahooMetadata, "args": {}},
    "Tradier": {"class": TradierMetadata, "args": {"key": config.get_value("tradier_api_key")}}
}


if __name__ == "__main__":

    cli = CLI(name="metadata_update",
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
        query = metadata_table.select().where(metadata_table.c.data_json.has_key('ticker')
                                              ).order_by(metadata_table.c.data_json['ticker'].astext)
        result = conn.execute(query)
    tickers = [row[1]['ticker'] for row in result.fetchall()]
    log.d("successfully read ticker list")

    # Start metadata download and upload
    log.i("beginning metadata retrieval with provider {} and override {}".format(provider, override))
    for ticker in tickers:
        log.i("working on {}".format(ticker))
        # Retrieve infos
        info = source.info([ticker])
        if info is False or len(info) == 0:
            log.i("{} not supported by {}".format(ticker, provider))
            continue
        info = info[0]  # TODO: handle multiple tickers info at once
        log.d("uploading {} metadata to db".format(ticker))
        with db_adapter.begin() as conn:
            old = db_adapter.get_tables()['metadata']
            query = metadata_table.update().values(data_json=func.jsonb_recursive_merge(old.c.data_json, json.dumps(info), override))\
                .where(metadata_table.c.data_json["ticker"].astext == ticker)
            conn.execute(query)
        log.d("upload {} completed".format(ticker))
