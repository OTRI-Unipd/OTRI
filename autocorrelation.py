from otri.filtering.filter_list import FilterList, FilterLayer
from otri.filtering.stream import Stream
from otri.filtering.filters.interpolation_filter import InterpolationFilter
from otri.filtering.filters.phase_filter import PhaseMulFilter, PhaseDeltaFilter
from otri.filtering.filters.math_filter import MathFilter
from otri.filtering.filters.statistics_filter import StatisticsFilter
from otri.filtering.filters.generic_filter import GenericFilter
from otri.database.postgresql_adapter import PostgreSQLAdapter, DatabaseQuery
from otri.config import Config
from pathlib import Path
from typing import Mapping, Collection
import matplotlib.pyplot as plt
import json
import time

DATABASE_TABLE = "atoms_b"
DB_TICKER_QUERY = "data_json->>'ticker' = '{}' AND data_json->>'provider' = 'yahoo finance' ORDER BY data_json->>'datetime'"
def query_lambda(ticker): return DB_TICKER_QUERY.format(ticker)


RUSSELL_3000_FILE = Path("docs/russell3000.json")

def autocorrelation(input_stream : Stream, atom_keys : Collection, distance : int = 1)->Mapping:
    '''
    Calculates autocorrelation of the given stream.

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
    # Filter list 1

    tuple_extractor = GenericFilter(
        source_stream=input_stream,
        operation=lambda element: element[0]
    )
    f_layer_tuple_ex = FilterLayer([tuple_extractor])

    interp_filter = InterpolationFilter(
        input_stream=tuple_extractor.get_output_stream(0),
        keys_to_change=atom_keys,
        target_interval="minutes"
    )
    f_layer_interp = FilterLayer([interp_filter])

    stats_filter = StatisticsFilter(
        input_stream=interp_filter.get_output_stream(0),
        keys=atom_keys
    ).calc_avg().calc_max()
    f_layer_stats = FilterLayer([stats_filter])

    f_list_1 = FilterList([
        f_layer_tuple_ex,
        f_layer_interp,
        f_layer_stats
    ])
    f_list_1.execute()

    # Filter list 2

    normalize_filter = MathFilter(
        input_stream=stats_filter.get_output_stream(0),
        keys_operations={k: lambda value: value/v
                            for k, v in stats_filter.get_max().items()}
    )
    f_layer_normalize = FilterLayer([normalize_filter])

    subtract_filter = MathFilter(
        input_stream=stats_filter.get_output_stream(0),
        keys_operations={k: lambda value: value - (stats_filter.get_avg()[k])#/stats_filter.get_max()[k])
                            for k in atom_keys}
    )
    f_layer_subtract = FilterLayer([subtract_filter])

    mul_filter = PhaseMulFilter(
        input_stream=subtract_filter.get_output_stream(0),
        keys_to_change=atom_keys,
        distance=distance
    )
    f_layer_mul = FilterLayer([mul_filter])

    integrator_filter = StatisticsFilter(
        input_stream=mul_filter.get_output_stream(0),
        keys=atom_keys
    ).calc_avg()
    f_layer_integ = FilterLayer([integrator_filter])

    f_list_2 = FilterList([
        #f_layer_normalize,
        f_layer_subtract,
        f_layer_mul,
        f_layer_integ
    ])

    f_list_2.execute()

    auto_correlation = integrator_filter.get_avg()
    return auto_correlation

def autocorrelation_delta(input_stream : Stream, atom_keys : Collection, distance : int = 1)->Mapping:
    '''
    Calculates autocorrelation of the given stream.

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

    # Filter list 1

    tuple_extractor = GenericFilter(
        source_stream=input_stream,
        operation=lambda element: element[0]
    )
    f_layer_tuple_ex = FilterLayer([tuple_extractor])

    interp_filter = InterpolationFilter(
        input_stream=tuple_extractor.get_output_stream(0),
        keys_to_change=atom_keys,
        target_interval="minutes"
    )
    f_layer_interp = FilterLayer([interp_filter])

    f_list_1 = FilterList([f_layer_tuple_ex, f_layer_interp])
    f_list_1.execute()

    delta_filter = PhaseDeltaFilter(
        input_stream=interp_filter.get_output_stream(0),
        keys_to_change=atom_keys,
        distance=1
    )
    f_layer_delta = FilterLayer([delta_filter])

    mul_filter = PhaseMulFilter(
        input_stream=delta_filter.get_output_stream(0),
        keys_to_change=atom_keys,
        distance=distance
    )
    f_layer_mul = FilterLayer([mul_filter])

    avg_filter = StatisticsFilter(
        input_stream=mul_filter.get_output_stream(0),
        keys=atom_keys
    ).calc_avg()
    f_layer_integ = FilterLayer([avg_filter])

    f_list_2 = FilterList([f_layer_delta,f_layer_mul,f_layer_integ])
    f_list_2.execute()

    auto_correlation = avg_filter.get_avg()
    return auto_correlation

KEYS_TO_CHANGE = ("open", "high", "low", "close")

if __name__ == "__main__":
    db_adapter = PostgreSQLAdapter(
        username=Config.get_config("postgre_username"),
        password=Config.get_config("postgre_password"),
        host=Config.get_config("postgre_host"))

    tickers_dict = json.load(RUSSELL_3000_FILE.open("r"))
    tickers = [ticker['ticker'] for ticker in tickers_dict['tickers']]
    for ticker in tickers:
        db_stream_1 = db_adapter.stream(DatabaseQuery(
            DATABASE_TABLE, query_lambda(ticker)))
        db_stream_2 = db_adapter.stream(DatabaseQuery(
            DATABASE_TABLE, query_lambda(ticker)))
        start_time = time.time()
        print("{} auto-correlation: {}".format(ticker,autocorrelation(db_stream_1, KEYS_TO_CHANGE).items()))
        print("Autocorr v1 took {} seconds to complete".format(time.time() - start_time))
        start_time = time.time()
        print("{} auto-correlation delta: {}".format(ticker,autocorrelation_delta(db_stream_2, KEYS_TO_CHANGE).items()))
        print("Autocorr v2 took {} seconds to complete".format(time.time() - start_time))
