'''
Module that performs analysis on database data.
'''

__author__ = "Luca Crema <lc.crema@hotmail.com>"
__version__ = "0.1"

from otri.utils import config, logger as log
from otri.database.postgresql_adapter import PostgreSQLAdapter
from otri.analysis.convergence import ConvergenceAnalysis
from sqlalchemy import or_, between

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

    ticker = "AAPL"

    analyser = ConvergenceAnalysis()

    with db_adapter.begin() as conn:
        a_t = db_adapter.get_tables()[ATOMS_TABLE]
        query = a_t.select()\
            .where(a_t.c.data_json['ticker'].astext == ticker)\
            .where(a_t.c.data_json['provider'].astext == "yahoo finance")\
            .where(
                or_(
                    a_t.c.data_json['type'].astext == 'price',
                    a_t.c.data_json['type'].astext == 'share price',
                )
            )\
            .where(between(a_t.c.data_json['Datetime'], '2020-08-07 08:00:00.000', '2020-08-07 20:00:00.000'))\
            .order_by(a_t.c.data_json['Datetime'])
        db_stream1 = db_adapter.stream(query, batch_size=1000)
        db_stream2 = db_adapter.stream(query, batch_size=1000)

    log.i("beginning convergence calc")
    log.i("convergence: {}".format(analyser.execute([db_stream1, db_stream2])))
