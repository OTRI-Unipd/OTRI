"""
Module to calculate autocorrelation within a single ticker at various distances.
"""

__autor__ = "Riccardo De Zen <riccardodezen98@gmail.com>, Luca Crema <lc.crema@hotmail.com>"
__version__ = "0.2"
__all__ = ['autocorrelation']

from otri.filtering.filter_list import FilterList, FilterLayer
from otri.filtering.stream import Stream
from otri.filtering.filters.interpolation_filter import InterpolationFilter
from otri.filtering.filters.phase_filter import PhaseMulFilter, PhaseDeltaFilter
from otri.filtering.filters.math_filter import MathFilter
from otri.filtering.filters.statistics_filter import StatisticsFilter
from otri.filtering.filters.generic_filter import GenericFilter
from otri.database.postgresql_adapter import PostgreSQLAdapter, DatabaseQuery
from otri.utils.config import Config
from pathlib import Path
from typing import Mapping, Collection
#import matplotlib.pyplot as plt
import json
import time

DATABASE_TABLE = "atoms_b"
DB_TICKER_QUERY = "data_json->>'ticker' = '{}' AND data_json->>'provider' = 'yahoo finance' ORDER BY data_json->>'datetime'"
def query_lambda(ticker): return DB_TICKER_QUERY.format(ticker)
RUSSELL_3000_FILE = Path("docs/russell3000.json")

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

    autocorr_list = FilterList([
        FilterLayer([
            # Tuple extractor
            GenericFilter(
                inputs="db_tuples",
                outputs="db_atoms",
                operation=lambda element: element[0]
            )
        ]),
        FilterLayer([
            # Interpolation
            InterpolationFilter(
                inputs="db_atoms",
                outputs="interp_atoms",
                keys_to_interp=atom_keys,
                target_interval="minutes"
            )
        ]),
        FilterLayer([
            # Delta
            PhaseDeltaFilter(
                inputs="interp_atoms",
                outputs="delta_atoms",
                keys_to_change=atom_keys,
                distance=1
            )
        ]),
        FilterLayer([
            # Phase multiplication
            PhaseMulFilter(
                inputs="delta_atoms",
                outputs="mult_atoms",
                keys_to_change=atom_keys,
                distance=distance
            )
        ]),
        FilterLayer([
            # Phase multiplication
            StatisticsFilter(
                inputs="mult_atoms",
                outputs="out_atoms",
                keys=atom_keys
            ).calc_avg("autocorrelation").calc_count("count")
        ])
    ]).execute({"db_tuples": input_stream})

    time_took = time.time() - start_time
    count_stats = autocorr_list.status("count",{})
    count = count_stats.get('close',0)

    print("Took {} seconds to compute {} atoms, {} atoms/second".format(
            time_took, count, count/time_took))

    return autocorr_list.status("autocorrelation",0)


KEYS_TO_CHANGE = ("open", "high", "low", "close")

if __name__ == "__main__":
    db_adapter = PostgreSQLAdapter(
        username=Config.get_config("postgre_username"),
        password=Config.get_config("postgre_password"),
        host=Config.get_config("postgre_host"))

    tickers_dict = json.load(RUSSELL_3000_FILE.open("r"))
    tickers = [ticker['ticker'] for ticker in tickers_dict['tickers']]
    for ticker in tickers:
        db_stream = db_adapter.stream(
            DatabaseQuery(DATABASE_TABLE, query_lambda(ticker)),
            batch_size=4000
        )

        print("{} auto-correlation: {}".format(ticker, autocorrelation(db_stream, KEYS_TO_CHANGE)))