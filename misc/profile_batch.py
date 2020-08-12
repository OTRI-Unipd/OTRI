from otri.database.postgresql_adapter import PostgreSQLAdapter

import otri.utils.config as config
import matplotlib.pyplot as plt
import time


def measure_streaming_time(batch_size: int) -> float:
    adapter = PostgreSQLAdapter(
        host=config.get_value("postgresql_host"),
        port=config.get_value("postgresql_port"),
        user=config.get_value("postgresql_username"),
        password=config.get_value("postgresql_password"),
        database=config.get_value("postgresql_database")
    )
    with adapter.session() as session:
        # SELECT * FROM atoms_b WHERE data_json->>'ticker' == 'AAPL';
        table = adapter.get_tables().atoms_b
        query = session.query(table).filter(table.data_json['ticker'].astext == 'AAPL')
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