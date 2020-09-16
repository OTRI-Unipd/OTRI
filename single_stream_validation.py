from otri.database.postgresql_adapter import PostgreSQLAdapter
from otri.analysis.find_negatives import NegativeAnalysis
from otri.analysis.find_null import NullAnalysis
from otri.analysis.find_clusters import ClusterAnalysis
from otri.analysis import db_share_query
from otri.utils import config
from otri.utils import logger as log

from progress.counter import Counter
from termcolor import colored
from pathlib import Path
import cProfile
import json

TICKER_FILE = Path("docs/snp100.json")
DATABASE_TABLE = "atoms_b"

NON_NULL_KEYS = {"open", "high", "low", "close", "volume", "datetime"}
NON_NEGATIVE_KEYS = {"open", "high", "low", "close", "volume"}
CLUSTER_KEYS = {"high", "low", "volume"}

elapsed_counter = None
atoms_counter = 0


def retrieve_tickers():
    tickers_dict = json.load(TICKER_FILE.open("r"))
    return [ticker['ticker'] for ticker in tickers_dict['tickers']]


def init_counter():
    global elapsed_counter
    elapsed_counter = Counter(colored("Atoms elapsed: ", "magenta"))


def update_counter():
    global atoms_counter
    global elapsed_counter
    atoms_counter += 1
    if(atoms_counter % 100 == 0):
        elapsed_counter.next(100)


def close_counter():
    global atoms_counter
    global elapsed_counter
    elapsed_counter.next(atoms_counter - elapsed_counter.index)
    elapsed_counter.finish()
    atoms_counter = 0


def manage_cluster_result(results):
    sum_per_key = dict()
    flags_per_key = dict()
    for ticker, result in results.items():
        for k, v in result["mean"].items():
            sum_per_key.setdefault(k, 0)
            sum_per_key[k] += v
        for k, v in result["flagged"].items():
            flags_per_key.setdefault(k, 0)
            flags_per_key[k] += v
    total_mean = {k: v/len(results.keys()) for k, v in sum_per_key.items()}
    return [flags_per_key, total_mean]


# The validations to run.
# ANALYSIS CLASS, PROVIDER, TICKER CALLABLE, OUTPUT FILE, MANAGE RESULTS
PROVIDERS = {"alpha vantage", "yahoo finance", "tradier"}
VALIDATION_PARAMS = [(
    ClusterAnalysis(CLUSTER_KEYS, update_counter),
    provider,
    retrieve_tickers,
    Path("log/{}_cluster.txt".format(provider)),
    manage_cluster_result
) for provider in PROVIDERS] + [(
    NegativeAnalysis(NON_NEGATIVE_KEYS, update_counter),
    provider,
    retrieve_tickers,
    Path("log/{}_non_neg.txt".format(provider)),
    lambda results: None
) for provider in PROVIDERS] + [(
    NullAnalysis(NON_NULL_KEYS, update_counter),
    provider,
    retrieve_tickers,
    Path("log/{}_non_null.txt".format(provider)),
    lambda results: None
) for provider in PROVIDERS]


if __name__ == "__main__":
    with cProfile.Profile() as pf:
        db_adapter = PostgreSQLAdapter(
            database=config.get_value("postgresql_database"),
            user=config.get_value("postgresql_username"),
            password=config.get_value("postgresql_password"),
            host=config.get_value("postgresql_host"),
            port=config.get_value("postgresql_port", "5432")
        )

        for params in VALIDATION_PARAMS:
            analysis = params[0]
            provider = params[1]
            get_tickers = params[2]
            output_file = params[3]
            manage_results = params[4]

            results_per_ticker = dict()
            flags_per_ticker = dict()
            percent_per_ticker = dict()

            for ticker in get_tickers():
                with db_adapter.session() as session:
                    atoms_table = db_adapter.get_classes()[DATABASE_TABLE]
                    query = db_share_query(session, atoms_table, ticker, provider)
                    db_stream = db_adapter.stream(query, batch_size=1000)
                log.d("Beginning {} for {}".format(analysis.__class__.__name__, ticker))
                init_counter()
                result, flagged, total, elapsed_time = analysis.execute([db_stream])
                close_counter()
                log.i("Completed in {} seconds.".format(elapsed_time))
                log.i("Result: {}, Flagged: {} / Total: {}, elapsed_time: {}".format(
                    result, flagged, total, elapsed_time
                ))
                if total > 0:
                    results_per_ticker[ticker] = result
                    log.i("Execution speed: {} atoms/s".format(total / elapsed_time))
                if flagged > 0:
                    flags_per_ticker[ticker] = flagged
                    percent_per_ticker[ticker] = flagged / total * 100

            with output_file.open("w+") as out:
                flagged_tickers = len(flags_per_ticker.keys())
                out.write("{} tickers had flagged values.\n".format(flagged_tickers))
                if flagged_tickers != 0:
                    out.write("{} average percentage of flags.".format(
                        sum(percent_per_ticker.values()) / len(results_per_ticker.keys())
                    ))
                out.write("Results: {}".format(manage_results(results_per_ticker)))

        pf.dump_stats("validation.prof")

    # os.system("python -m snakeviz validation.prof")
