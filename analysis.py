'''
Module that performs analysis on database data.
'''

__author__ = "Luca Crema <lc.crema@hotmail.com>"
__version__ = "0.1"

from otri.utils import config, logger as log
from otri.database.postgresql_adapter import PostgreSQLAdapter
from otri.analysis.convergence import ConvergenceAnalysis
from sqlalchemy import or_, between
import json

ATOMS_TABLE = "atoms_b"

if __name__ == "__main__":

    db_adapter = PostgreSQLAdapter(
        host=config.get_value("postgresql_host"),
        port=config.get_value("postgresql_port", "5432"),
        user=config.get_value("postgresql_username", "postgres"),
        password=config.get_value("postgresql_password"),
        database=config.get_value("postgresql_database", "postgres")
    )

    # TODO : refactor to use other analysis

    tickers = ("VUSA.MI", "IUSA.MI")
    db_streams = []

    analyser = ConvergenceAnalysis()

    with db_adapter.session() as session:
        a_t = db_adapter.get_classes()[ATOMS_TABLE]
        for ticker in tickers:
            query = session.query(a_t)\
                .filter(a_t.data_json['ticker'].astext == ticker)\
                .filter(a_t.data_json['provider'].astext == "yahoo finance")\
                .filter(
                    or_(
                        a_t.data_json['type'].astext == 'price',
                        a_t.data_json['type'].astext == 'share price',
                    )
            )\
                .filter(between(a_t.data_json['Datetime'].astext, '2020-08-01 08:00:00.000', '2020-08-12 20:00:00.000'))\
                .order_by(a_t.data_json['Datetime'])
            db_streams.append(db_adapter.stream(query, batch_size=1000))

    log.i("beginning convergence calc")
    log.i("Average rate: {}".format(json.dumps(analyser.execute(db_streams), indent=4)))
