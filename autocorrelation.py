"""
Module to calculate autocorrelation within a single ticker at various distances.
"""

__author__ = "Riccardo De Zen <riccardodezen98@gmail.com>, Luca Crema <lc.crema@hotmail.com>"
__version__ = "0.2"
__all__ = ['autocorrelation']

from otri.filtering.filter_net import FilterNet, FilterLayer, EXEC_AND_PASS, BACK_IF_NO_OUTPUT
from otri.filtering.stream import Stream
from otri.filtering.filters.interpolation_filter import IntradayInterpolationFilter
from otri.filtering.filters.phase_filter import PhaseMulFilter, PhaseDeltaFilter
from otri.filtering.filters.statistics_filter import StatisticsFilter
from otri.filtering.filters.generic_filter import GenericFilter
from otri.filtering.filters.summary_filter import SummaryFilter
from otri.database.postgresql_adapter import PostgreSQLAdapter
from otri.utils import config, key_handler as kh, logger as log
from sqlalchemy.orm.session import Session
from sqlalchemy.orm.query import Query
from pathlib import Path
from typing import Mapping, Collection
from progress.counter import Counter
from termcolor import colored
import json
import time

DATABASE_TABLE = "atoms_b"


def db_ticker_query(session: Session, atoms_table: str, ticker: str) -> Query:
    '''
    Return a Query object for the given session on the given table. Query can be read as follows:
    ```sql
    SELECT * FROM [atoms_table]
    WHERE data_json->>'ticker' = '[ticker]'
    AND data_json->>'provider' = 'yahoo finance'
    ORDER BY data_json->>'datetime'
    ```

    Parameters:
        session : Session
            Database session for the query.\n
        atoms_table : str
            The table containing the atoms. Must have a structure like: (int, json/jsonb), with the
            json field being called "data_json". Can also be directly a table mapped class.
        ticker : str
            The ticker for which to retrieve the atoms.\n
    Returns:
        An sqlalchemy query as described above.
    '''
    t = atoms_table
    return session.query(t).filter(
        t.data_json['ticker'].astext == ticker
    ).filter(
        t.data_json['provider'].astext == 'yahoo finance'
    ).filter(
        t.data_json['type'].astext == 'share price'
    ).order_by(t.data_json['datetime'].astext)


RUSSELL_3000_FILE = Path("docs/russell3000.json")

elapsed_counter = None
atoms_counter = 0


def on_data_output():
    global atoms_counter
    atoms_counter += 1
    if(atoms_counter % 10 == 0):
        elapsed_counter.next(10)


def autocorrelation(input_stream: Stream, atom_keys: Collection, distance: int = 1) -> Mapping:
    '''
    Calculates autocorrelation of the given stream using the difference between atoms values.

    Parameters:
        input_stream : Stream
            Stream of atoms from the same ticker ordered by timestamp.
        atom_keys : Collection
            Collection of keys to calculate autocorrelation of.
        distance : int
            Autocorrelation distance in minutes.
            Value of c in a[i] * a[i+c].
    Returns:
        Mapping containing value of autocorrelation for given keys.
    '''

    start_time = time.time()
    global elapsed_counter
    elapsed_counter = Counter(colored("Atoms elapsed: ", "magenta"))
    autocorr_net = FilterNet([
        FilterLayer([
            # To Lowercase
            GenericFilter(
                inputs="input_atoms",
                outputs="lower_atoms",
                operation=lambda atom: kh.lower_all_keys_deep(atom)
            )
        ], BACK_IF_NO_OUTPUT),
        FilterLayer([
            # Tuple extractor
            SummaryFilter(
                inputs="lower_atoms",
                outputs="summarized_atoms",
                state_name="Statistics"
            )
        ], EXEC_AND_PASS),
        FilterLayer([
            # Interpolation
            IntradayInterpolationFilter(
                inputs="summarized_atoms",
                outputs="interp_atoms",
                interp_keys=atom_keys,
                constant_keys=["ticker", "provider"],
                target_gap_seconds=60
            )
        ], BACK_IF_NO_OUTPUT),
        FilterLayer([
            # Delta
            PhaseDeltaFilter(
                inputs="interp_atoms",
                outputs="delta_atoms",
                keys_to_change=atom_keys,
                distance=1
            )
        ], BACK_IF_NO_OUTPUT),
        FilterLayer([
            # Phase multiplication
            PhaseMulFilter(
                inputs="delta_atoms",
                outputs="mult_atoms",
                keys_to_change=atom_keys,
                distance=distance
            )
        ], BACK_IF_NO_OUTPUT),
        FilterLayer([
            # Phase multiplication
            StatisticsFilter(
                inputs="mult_atoms",
                outputs="out_atoms",
                keys=atom_keys
            ).calc_avg("autocorrelation").calc_count("count")
        ], EXEC_AND_PASS)
    ]).execute({"input_atoms": input_stream}, on_data_output=on_data_output)

    time_took = time.time() - start_time
    count_stats = autocorr_net.state("count", {})
    count = count_stats.get('close', 0)

    elapsed_counter.finish()
    if not count:
        log.d("no atoms found, took {} seconds.".format(time_took))
    else:
        log.d("took {} seconds to compute {} atoms, {} atoms/second".format(
            time_took, count, count/time_took
        ))

    log.d("Stats: {}".format(autocorr_net.state("Statistics", "Nope")))

    return autocorr_net.state("autocorrelation", 0)


KEYS_TO_CHANGE = ("open", "high", "low", "close")

if __name__ == "__main__":

    db_adapter = PostgreSQLAdapter(
        database=config.get_value("postgresql_database"),
        user=config.get_value("postgresql_username"),
        password=config.get_value("postgresql_password"),
        host=config.get_value("postgresql_host"),
        port=config.get_value("postgresql_port", "5432")
    )

    tickers_dict = json.load(RUSSELL_3000_FILE.open("r"))
    tickers = [ticker['ticker'] for ticker in tickers_dict['tickers']]
    for ticker in tickers:
        with db_adapter.session() as session:
            atoms_table = db_adapter.get_classes()[DATABASE_TABLE]
            query = db_ticker_query(session, atoms_table, ticker)
            db_stream = db_adapter.stream(query, batch_size=1000, extract_atom=True)
        log.i("Beginning autocorr calc for {}".format(ticker))
        log.i("{} auto-correlation: {}".format(ticker, autocorrelation(db_stream, KEYS_TO_CHANGE)))
