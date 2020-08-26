from otri.database.postgresql_adapter import PostgreSQLAdapter
from otri.utils import config, logger as log, key_handler as kh
from otri.filtering.stream import Stream

import json
from pathlib import Path
from typing import Mapping
from sqlalchemy.orm.session import Session
from sqlalchemy.orm.query import Query
import time
from progress.counter import Counter
from termcolor import colored

from otri.filtering.filter_net import BACK_IF_NO_OUTPUT, EXEC_AND_PASS, FilterNet
from otri.filtering.filter_layer import FilterLayer
from otri.filtering.filters.generic_filter import GenericFilter

from otri.validation import MonoValidator
from otri.validation.exceptions import NullError
from otri.validation.valchecks import check_non_null

RUSSELL_3000_FILE = Path("docs/russell3000.json")
DATABASE_TABLE = "atoms_b"

elapsed_counter = None
atoms_counter = 0


def on_data_output():
    global atoms_counter
    atoms_counter += 1
    if(atoms_counter % 10 == 0):
        elapsed_counter.next(10)


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
        t.data_json['type'].astext == 'price'
    ).order_by(t.data_json['datetime'].astext)


def analysis(input_stream: Stream) -> Mapping:
    '''
    Analysis: find all atoms with null important keys.

    Parameters:
        input_stream : Stream
            Stream of atoms from the same ticker ordered by timestamp.

    Returns:
        The count of flagged atoms and of total atoms.
    '''
    global elapsed_counter
    elapsed_counter = Counter(colored("Atoms elapsed: ", "magenta"))

    start_time = time.time()
    analysis_net = FilterNet([
        FilterLayer([
            # Tuple extractor
            GenericFilter(
                inputs="db_tuples",
                outputs="db_atoms",
                operation=lambda element: element[1]
            )
        ], EXEC_AND_PASS),
        FilterLayer([
            # To Lowercase
            GenericFilter(
                inputs="db_atoms",
                outputs="lower_atoms",
                operation=lambda atom: kh.lower_all_keys_deep(atom)
            )
        ], BACK_IF_NO_OUTPUT),
        FilterLayer([
            # Check Non-null
            MonoValidator(
                inputs="lower_atoms",
                outputs="output",
                check=lambda atom: check_non_null(
                    atom, ["open", "high", "low", "close", "ticker", "datetime", "provider"]
                )
            )
        ], EXEC_AND_PASS)
    ]).execute({"db_tuples": input_stream}, on_data_output=on_data_output)

    time_took = time.time() - start_time
    output = list(analysis_net.streams()["output"])

    total = len(output)
    flagged = len(list(filter(lambda x: NullError.KEY in x.keys(), output)))

    elapsed_counter.finish()
    if not total:
        log.d("no atoms found, took {} seconds.".format(time_took))
    else:
        log.d("took {} seconds to compute {} atoms, {} atoms/second".format(
            time_took, total, total / time_took
        ))

    return flagged, total


if __name__ == "__main__":

    db_adapter = PostgreSQLAdapter(
        database=config.get_value("postgresql_database"),
        user=config.get_value("postgresql_username"),
        password=config.get_value("postgresql_password"),
        host=config.get_value("postgresql_host"),
        port=config.get_value("postgresql_port", "5432")
    )

    flagged_tickers = dict()

    tickers_dict = json.load(RUSSELL_3000_FILE.open("r"))
    tickers = [ticker['ticker'] for ticker in tickers_dict['tickers']]
    for ticker in tickers:
        with db_adapter.session() as session:
            atoms_table = db_adapter.get_classes()[DATABASE_TABLE]
            query = db_ticker_query(session, atoms_table, ticker)
            db_stream = db_adapter.stream(query, batch_size=1000)
        log.i("Beginning autocorr calc for {}".format(ticker))
        flagged, total = analysis(db_stream)
        if total != 0:
            percent = flagged / total * 100
            log.i("{} null percent: {}%".format(ticker, percent))
            if percent > 0:
                flagged_tickers[ticker] = percent

    log.i("Tickers with null values: {}".format(flagged_tickers))
