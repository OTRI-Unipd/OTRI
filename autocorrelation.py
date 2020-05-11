from otri.filtering.filter_list import FilterList, FilterLayer
from otri.filtering.stream import Stream
from otri.filtering.filters.interpolation_filter import InterpolationFilter
from otri.filtering.filters.multiplier_filter import MultiplierFilter
from otri.filtering.filters.math_filter import MathFilter
from otri.filtering.filters.average_filter import AverageFilter
from otri.filtering.filters.generic_filter import GenericFilter
from otri.database.postgresql_adapter import PostgreSQLAdapter, DatabaseQuery
from otri.config import Config
from pathlib import Path
import matplotlib.pyplot as plt
import json
import time

DATABASE_TABLE = "atoms_b"
DB_TICKER_QUERY = "data_json->>'ticker' = '{}' AND data_json->>'provider' = 'yahoo finance' ORDER BY data_json->>'datetime'"
query_lambda = lambda ticker : DB_TICKER_QUERY.format(ticker)
RUSSELL_3000_FILE = Path("docs/russell3000.json")

def on_atom(atom):
    closes.append(atom['close'])

def on_finished():
    end_time = time.time()
    seconds = end_time - start_time
    print("it took {} seconds".format(seconds))
    #plt.plot(closes)
    #plt.xticks(rotation=90)
    #plt.show()

KEYS_TO_CHANGE = ("open", "high", "low", "close")

if __name__ == "__main__":
    closes = list()

    db_adapter = PostgreSQLAdapter(
        username=Config.get_config("postgre_username"),
        password=Config.get_config("postgre_password"),
        host=Config.get_config("postgre_host"))

    tickers_dict = json.load(RUSSELL_3000_FILE.open("r"))
    tickers = [ticker['ticker'] for ticker in tickers_dict['tickers']]
    for ticker in tickers:
        db_stream = db_adapter.stream(DatabaseQuery(DATABASE_TABLE,query_lambda("AAPL")))

        # Filter list 1

        tuple_extractor = GenericFilter(
            source_stream=db_stream,
            operation=lambda element: element[0]
        )
        f_layer_tuple_ex = FilterLayer([tuple_extractor])

        interp_filter = InterpolationFilter(
            input_stream=tuple_extractor.get_output_stream(0),
            keys_to_change=KEYS_TO_CHANGE,
            target_interval="minutes"
        )
        f_layer_interp = FilterLayer([interp_filter])

        avg_filter = AverageFilter(
            input_stream=interp_filter.get_output_stream(0),
            keys=KEYS_TO_CHANGE
        )
        f_layer_avg = FilterLayer()
        f_layer_avg.append(avg_filter)

        f_list_1 = FilterList([f_layer_tuple_ex, f_layer_interp, f_layer_avg])
        f_list_1.execute()

        # Filter list 2

        subtract_filter = MathFilter(
            input_stream=avg_filter.get_output_stream(0),
            keys_operations={k: lambda value: value-v for k,v in avg_filter.get_avgs().items()}
        )
        f_layer_subtract = FilterLayer([subtract_filter])

        normalize_filter = MathFilter(
            input_stream=subtract_filter.get_output_stream(0),
            keys_operations={k: lambda value: value/315 for k,v in avg_filter.get_avgs().items()}
        )
        f_layer_normalize = FilterLayer([normalize_filter])

        mul_filter = MultiplierFilter(
            input_stream=normalize_filter.get_output_stream(0),
            keys_to_change=KEYS_TO_CHANGE,
            distance=1
        )
        f_layer_mul = FilterLayer([mul_filter])

        integrator_filter = AverageFilter(
            input_stream=mul_filter.get_output_stream(0),
            keys=KEYS_TO_CHANGE
        )
        f_layer_integ = FilterLayer([integrator_filter])

        f_list_2 = FilterList([f_layer_subtract, f_layer_normalize, f_layer_mul, f_layer_integ])

        start_time = time.time()
        f_list_2.execute(None, on_finished)
        
        
        auto_correlation = integrator_filter.get_avgs()

        print("{} auto-correlation: {}".format(ticker, {k:v/100 for k,v in auto_correlation})

