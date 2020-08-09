from otri.database.postgresql_adapter import PostgreSQLAdapter
from otri.database.database_query import DatabaseQuery

import otri.utils.config as config
import matplotlib.pyplot as plt
import time


def measure_streaming_time(batch_size: int) -> float:
    adapter = PostgreSQLAdapter(
        config.get_value("postgre_username"),
        config.get_value("postgre_password"),
        config.get_value("postgre_host")
    )
    query = DatabaseQuery("atoms_b", "data_json->>'ticker' = 'AAPL'")
    stream = adapter.stream(query, batch_size)
    count = 0
    start = time.time()
    db_iter = stream.__iter__()
    while db_iter.has_next():
        row = db_iter.__next__()
        if not row:
            print("Found empty line, check correctness...")
        count += 1
    end = time.time()
    adapter.close()
    seconds = end - start
    print("Took me {} seconds to stream {} rows with a batch of size {}".format(
        seconds, count, batch_size))
    return end - start


if __name__ == "__main__":
    sizes = list(range(10, 10001, 10))
    times = [measure_streaming_time(s) for s in sizes]
    fig, ax = plt.subplots()
    ax.plot(sizes, times)
    ax.set(xlabel='batch size (rows)', ylabel='stream time (seconds)',
           title='full database stream scan time')
    ax.grid()
    plt.show()