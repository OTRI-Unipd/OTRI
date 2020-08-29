from otri.utils.cartesian_hashtable import CartesianHashTable

import os
import matplotlib.pyplot as plt
from time import time
from cProfile import Profile
from numpy.random import randint as rand
from typing import Iterable, List, Mapping, Tuple
from numbers import Real


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


def coordinates(item: Iterable[Real]) -> Tuple[Real]:
    '''
    Returns:
        A Tuple with the values for the keys "x", "y" and "z" of the item.
    '''
    return (item["x"], item["y"], item["z"])


def are_near(item: Iterable[Real], other: Iterable[Real], approx: Real) -> bool:
    '''
    Return True if the coordinates of the two items are near enough.

    Parameters:
        item : Iterable[Real]
            Item's coordinates are used as the 100%.
    '''
    approx = abs(approx)
    item_coords = coordinates(item)
    other_coords = coordinates(other)
    for i in range(len(item_coords)):
        if not ((1 - approx) * item_coords[i] <= other_coords[i] <= (1 + approx) * item_coords[i]):
            return False
    return True


if __name__ == "__main__":

    size_axis = list(range(500, 5001, 500))

    dict_times = list()
    table_times = list()

    approx = 0.25

    with Profile() as pf:
        for test_size in size_axis:
            test_data = random_data(test_size)
            split = rand(len(test_data) // 3, (len(test_data) * 2) // 3)
            inside = test_data[:split:]
            outside = test_data[split::]
            dictionary = dict()
            hashtable = CartesianHashTable(coordinates)

            # Add the items.
            for item in inside:
                dictionary[coordinates(item)] = item
                hashtable.add(item)
            print("Added")

            dict_start = time()
            for item in outside:
                for v in dictionary.values():
                    if are_near(item, v, approx):
                        break
            dict_end = time()
            dict_times.append(dict_end - dict_start)

            table_start = time()
            for item in outside:
                has_near = hashtable.near(item, approx)
            table_end = time()
            table_times.append(table_end - table_start)

            print("Dict done in: ", dict_times[-1])
            print("Table done in: ", table_times[-1])
            print("Finished: ", test_size)

        pf.dump_stats("neigh_profile.prof")

plt.plot(size_axis, table_times, '-go')
plt.plot(size_axis, dict_times, '-bs')
os.system("python -m snakeviz neigh_profile.prof")
