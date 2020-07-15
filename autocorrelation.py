"""
Module to calculate autocorrelation within a single ticker at various distances.
"""

__autor__ = "Riccardo De Zen <riccardodezen98@gmail.com>, Luca Crema <lc.crema@hotmail.com>"
__version__ = "0.2"
__all__ = ['autocorrelation']

from otri.filtering.filter_net import FilterNet, FilterLayer, EXEC_AND_PASS, BACK_IF_NO_OUTPUT
from otri.filtering.stream import Stream
from otri.filtering.filters.interpolation_filter import InterpolationFilter
from otri.filtering.filters.phase_filter import PhaseMulFilter, PhaseDeltaFilter
from otri.filtering.filters.statistics_filter import StatisticsFilter
from otri.filtering.filters.generic_filter import GenericFilter
from otri.database.postgresql_adapter import PostgreSQLAdapter, DatabaseQuery
from otri.utils import config, logger as log
from pathlib import Path
from typing import Mapping, Collection
from progress.counter import Counter
from termcolor import colored
import json
import time

DATABASE_TABLE = "atoms_b"
DB_TICKER_QUERY = "data_json->>'ticker' = '{}' AND data_json->>'provider' = 'yahoo finance' ORDER BY data_json->>'datetime'"
def query_lambda(ticker): return DB_TICKER_QUERY.format(ticker)
RUSSELL_3000_FILE = Path("docs/russell3000.json")

elapsed_counter = None
atoms_counter = 0

def on_data_output():
    global atoms_counter
    atoms_counter+=1
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
            # Tuple extractor
            GenericFilter(
                inputs="db_tuples",
                outputs="db_atoms",
                operation=lambda element: element[0]
            )
        ], EXEC_AND_PASS),
        FilterLayer([
            # Interpolation
            InterpolationFilter(
                inputs="db_atoms",
                outputs="interp_atoms",
                keys_to_interp=atom_keys,
                target_interval="minutes"
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
    ]).execute({"db_tuples": input_stream}, on_data_output=on_data_output)

    time_took = time.time() - start_time
    count_stats = autocorr_net.state("count",{})
    count = count_stats.get('close',0)

    elapsed_counter.finish()
    log.d("Took {} seconds to compute {} atoms, {} atoms/second".format(
            time_took, count, count/time_took))

    return autocorr_net.state("autocorrelation",0)


KEYS_TO_CHANGE = ("open", "high", "low", "close")

if __name__ == "__main__":
    db_adapter = PostgreSQLAdapter(
        username=config.get_value("postgre_username"),
        password=config.get_value("postgre_password"),
        host=config.get_value("postgre_host"))

    tickers_dict = json.load(RUSSELL_3000_FILE.open("r"))
    tickers = [ticker['ticker'] for ticker in tickers_dict['tickers']]
    for ticker in tickers:
        db_stream = db_adapter.stream(
            DatabaseQuery(DATABASE_TABLE, query_lambda(ticker)),
            batch_size=1000
        )
        log.i("Beginning autocorr calc for {}".format(ticker))
        log.i("{} auto-correlation: {}".format(ticker, autocorrelation(db_stream, KEYS_TO_CHANGE)))

