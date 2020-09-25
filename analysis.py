'''
Module that performs analysis on database data.
'''

__author__ = "Luca Crema <lc.crema@hotmail.com>"
__version__ = "0.1"

import json
from datetime import timedelta

from sqlalchemy import between, func
from sqlalchemy.orm.session import Session

from otri.analysis.convergence import ConvergenceAnalysis
from otri.database.postgresql_adapter import PostgreSQLAdapter
from otri.utils import config
from otri.utils import logger as log

ATOMS_TABLE = "atoms_b"
METADATA_TABLE = "metadata"


def build_query(session: Session, at, ticker: str):
    '''
    Builds an atoms query.\n

    Parameters:\n
        session : sqlalchemy.session\n
        at : sqlalchemy.table\n
            Atoms table
        ticker : str\n
            Ticker identifier
    '''
    return session.query(at).filter(at.data_json['ticker'].astext == ticker)\
        .filter(at.data_json['provider'].astext == "tradier")\
        .filter(at.data_json['type'].astext.in_(['price', 'share price']))\
        .order_by(at.data_json['Datetime'])


if __name__ == "__main__":

    db_adapter = PostgreSQLAdapter(
        host=config.get_value("postgresql_host"),
        port=config.get_value("postgresql_port", "5432"),
        user=config.get_value("postgresql_username", "postgres"),
        password=config.get_value("postgresql_password"),
        database=config.get_value("postgresql_database", "postgres")
    )

    # TODO : refactor to use other analysis
    tickers = []
    with db_adapter.begin() as conn:
        mt = db_adapter.get_tables()[METADATA_TABLE]
        query = mt.select().where(mt.c.data_json['provider'].contains('\"tradier\"'))\
            .where(func.lower(mt.c.data_json['type'].astext) == "etf")\
            .where(func.lower(mt.c.data_json['underlying'].astext) == "s&p 500")\
            .where(func.upper(mt.c.data_json['currency'].astext).in_(['USD', 'EUR']))\
            .order_by(mt.c.data_json['ticker'])
        for atom in conn.execute(query).fetchall():
            tickers.append(atom.data_json['ticker'])

    log.i("found {} tickers".format(len(tickers)))

    analyser = ConvergenceAnalysis(
        group_resolution=timedelta(hours=1),
        ratio_interval=timedelta(days=1),
        samples_precision=0
    )

    for i, ticker_one in enumerate(tickers):
        for j, ticker_two in enumerate(tickers):
            if ticker_one is ticker_two:
                continue
            with db_adapter.session() as session:
                at = db_adapter.get_classes()[ATOMS_TABLE]
                query_one = build_query(session, at, ticker_one)
                query_two = build_query(session, at, ticker_two)
                db_stream_one = db_adapter.stream(query_one, batch_size=1000)
                db_stream_two = db_adapter.stream(query_two, batch_size=1000)

            log.i("convergence analysis for {} and {}".format(ticker_one, ticker_two))
            log.i("results: {}".format(json.dumps(analyser.execute([db_stream_one, db_stream_two]), indent=4)))