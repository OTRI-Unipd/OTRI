from otri.utils.cartesian_hashtable import CartesianHashTable

import matplotlib.pyplot as plt
from time import time
from typing import List, Mapping
from numpy.random import randint as rand


def random_data(size: int) -> List[Mapping]:
    '''
    Parameters:
        size : int
            The size of the test data.

    Returns:
        List[Mapping] : The test data, a list of dictionaries {"x" : vX, "y" : vY, "z" : vZ}
    '''
    # Values on same magnitude of size.
    random_x = rand(-size * 5, size * 5, size)
    random_y = rand(-size * 5, size * 5, size)
    random_z = rand(-size * 5, size * 5, size)
    return [{"x": random_x[i], "y":random_y[i], "z":random_z[i]}
            for i in range(size)]


def coordinates(x: Mapping):
    return (x["x"], x["y"], x["z"])


if __name__ == "__main__":

    size_axis = list(range(1000, 15001, 1000))

    table_ins_time = list()
    dict_ins_time = list()

    table_iter_time = list()
    dict_iter_time = list()

    table_find_time = list()
    dict_find_time = list()

    table_remove_time = list()
    dict_remove_time = list()

    for test_size in size_axis:
        data = random_data(test_size)
        table = CartesianHashTable(coordinates, 3, (5 * test_size,) * 3, (-5 * test_size,) * 3)
        dictionary = dict()

        # Measure total insertion times.
        table_start = time()
        for item in data:
            table.add(item)
        table_end = time()
        table_ins_time.append(table_end - table_start)

        dict_start = time()
        for item in data:
            dictionary[coordinates(item)] = item
        dict_end = time()
        dict_ins_time.append(dict_end - dict_start)

        # Measure __iter__ time.
        table_start = time()
        for item in table:
            pass
        table_end = time()
        table_iter_time.append(table_end - table_start)

        dict_start = time()
        for item in dictionary.values():
            pass
        dict_end = time()
        dict_iter_time.append(dict_end - dict_start)

        # Measure total find time.
        table_start = time()
        for item in data:
            if item in table:
                pass
        table_end = time()
        table_find_time.append(table_end - table_start)

        dict_start = time()
        for item in data:
            if coordinates(item) in dictionary.keys():
                pass
        dict_end = time()
        dict_find_time.append(dict_end - dict_start)

        # Measure total removal time.
        table_start = time()
        for item in data:
            table.remove(item)
        table_end = time()
        table_remove_time.append(table_end - table_start)

        dict_start = time()
        for item in data:
            dictionary.pop(coordinates(item))
        dict_end = time()
        dict_remove_time.append(dict_end - dict_start)

        print("Done: ", test_size)

    f, axs = plt.subplots(2, 2)
    ax1 = axs[0, 0]
    ax2 = axs[0, 1]
    ax3 = axs[1, 0]
    ax4 = axs[1, 1]
    for ax in axs.flat:
        ax.set(xlabel='Items', ylabel='Time (seconds)')

    ax1.plot(size_axis, table_ins_time, '-go')
    ax1.plot(size_axis, dict_ins_time, '-bs')
    ax1.set_title('Insertion time')

    ax2.plot(size_axis, table_iter_time, '-go')
    ax2.plot(size_axis, dict_iter_time, '-bs')
    ax2.set_title('Iteration time')

    ax3.plot(size_axis, table_find_time, '-go')
    ax3.plot(size_axis, dict_find_time, '-bs')
    ax3.set_title('Find time')

    ax4.plot(size_axis, table_remove_time, '-go')
    ax4.plot(size_axis, dict_remove_time, '-bs')
    ax4.set_title('Remove time')

    plt.show()
