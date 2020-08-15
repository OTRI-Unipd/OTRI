'''
Module that performs analysis on database data.
'''

__author__ = "Luca Crema <lc.crema@hotmail.com>"
__version__ = "0.1"

from otri.utils import config, logger as log
from otri.database.postgresql_adapter import PostgreSQLAdapter
from otri.analysis.convergence import ConvergenceAnalysis

ATOMS_TABLE = "atoms_b"

if __name__ == "__main__":

    db_adapter = PostgreSQLAdapter(
        database=config.get_value("postgresql_database"),
        user=config.get_value("postgresql_username"),
        password=config.get_value("postgresql_password"),
        host=config.get_value("postgresql_host"),
        port=config.get_value("postgresql_port", "5432")
    )

    # TODO : refactor to use other analysis

    ticker = "AAPL"

    with db_adapter.begin() as conn:
        a_t = db_adapter.get_tables()[ATOMS_TABLE]
        query = a_t.select()\
                    .where(a_t.c.data_json['ticker'].astext == ticker)\
                    .where(a_t.c.data_json)\
                    .where(a_t.c.data_json['type'].astext == 'share price')
