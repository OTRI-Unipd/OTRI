__author__ = "Riccardo De Zen <riccardodezen98@gmail.com>"
__version__ = "1.0"

from otri.utils.cartesian_hashtable import CartesianHashTable

from typing import Iterable, Any, Callable
from numbers import Real
import numpy
import unittest
from parameterized import parameterized_class, parameterized


def origin(item: Any):
    '''
    Return two axis cartesian origin.
    '''
    return (0, 0)


def cartesian_tuple(item: Iterable[Real]):
    '''
    Return the item itself converted to a tuple.
    '''
    return tuple(item)


def are_near(item: Iterable[Real], other: Iterable[Real], approx: Real, coords: Callable) -> bool:
    '''
    Return True if the coordinates of the two items are near enough.

    Parameters:
        item : Iterable[Real]
            Item's coordinates are used as the 100%.
    '''
    approx = abs(approx)
    item_coords = coords(item)
    other_coords = coords(other)
    for i in range(len(item_coords)):
        bounds = [(1 - approx) * item_coords[i], (1 + approx) * item_coords[i]]
        if not min(bounds) <= other_coords[i] <= max(bounds):
            return False
    return True


class CartesianHashTableTest(unittest.TestCase):

    def setUp(self):
        '''
        Set up simple table with `origin` as coordinates method.
        '''
        self.table = CartesianHashTable(origin)

    def test_get_coordinates(self):
        '''
        Ensure that after object creation the method to retrieve the coordinates is available.
        '''
        self.assertIs(self.table.get_coordinates, origin)

    def test_size_zero_on_creation(self):
        '''Size must be zero at creation.'''
        self.assertEqual(self.table.size(), 0)

    def test_add_one(self):
        '''
        Ensure that a single item can be added.
        '''
        self.table.add("Hello I'm just an item.")
        self.assertEqual(self.table.size(), 1)

    def test_remove_one(self):
        '''
        Ensure the item can be removed.
        '''
        msg = "Hello I'm just an item."
        self.table.add(msg)
        self.table.remove(msg)
        self.assertEqual(self.table.size(), 0)

    def test_remove_value_error(self):
        '''
        Ensure ValueError is raised if trying to remove an item that isn't there.
        '''
        inside = "Hello, I am indeed part of the table."
        outside = "Hello, I am totally not part of the table."
        self.table.add(inside)
        self.assertRaises(ValueError, self.table.remove, outside)

    def test_contains(self):
        '''
        Ensure the contains method finds the inserted values.
        '''
        msg = "Hello I'm just an item."
        self.table.add(msg)
        self.assertIn(msg, self.table)

    def test_contains_if_not_contained(self):
        '''
        Ensure the contains method won't find a value that isn't there.
        '''
        msg = "Hello I'm just an item."
        self.table.add(msg)
        self.table.remove(msg)
        self.assertNotIn(msg, self.table)

    def test_to_scatter(self):
        '''
        Ensure the scatter lists contain only the inserted elements.
        '''
        self.table.add("Hi I'm a scatter plot.")
        for axis in self.table.scatter():
            self.assertListEqual(axis, [0])

    def test_iter(self):
        '''
        Test the iterator yields all inserted items.
        '''
        msg = "Hello I'm just an item."
        self.table.add(msg)
        self.assertListEqual(list(self.table), [msg])

    def test_border_index(self):
        '''
        Test you can add items in the first unavailable index.
        Default table size is 10^n, so 10 is the first unavailable index.
        '''
        self.table = CartesianHashTable(cartesian_tuple)
        self.table.add((10, 10, 10))
        pass


@parameterized_class(("min_value", "max_value"), [
    (0, 1000),
    (-1000, 1000),
    (-1000, 0)
])
class CartesianHashTableRandomTest(unittest.TestCase):

    '''
    Tests similar to above, but on three axes.
    '''

    def setUp(self):
        '''
        Set up simple table with `cartesian_tuple` as coordinates method.
        '''
        self.table = CartesianHashTable(cartesian_tuple)
        # A hundred items, on three dimensions, with coordinates from 0 to 100.
        self.dataset = [tuple(x) for x in numpy.random.randint(
            self.min_value, self.max_value, (1000, 5))]

    def test_size_zero_on_creation(self):
        '''Size must be zero at creation.'''
        self.assertEqual(self.table.size(), 0)

    def test_add_some(self):
        '''
        Ensure that a bunch of items be added.
        '''
        for item in self.dataset:
            self.table.add(item)
        self.assertEqual(self.table.size(), len(self.dataset))

    def test_remove_some(self):
        '''
        Ensure the item can be removed.
        '''
        for item in self.dataset:
            self.table.add(item)
        for item in self.dataset:
            self.table.remove(item)
        self.assertEqual(self.table.size(), 0)

    def test_remove_value_error(self):
        '''
        Ensure ValueError is raised if trying to remove an item that isn't there.
        '''
        # Divide in two parts. From one third to two thirds go in the table, the rest stays out.
        split = numpy.random.randint(len(self.dataset) // 3, (len(self.dataset) * 2) // 3)
        inside = self.dataset[:split:]
        outside = self.dataset[split::]

        for i in inside:
            self.table.add(i)

        for o in outside:
            self.assertRaises(ValueError, self.table.remove, o)

    def test_contains(self):
        '''
        Ensure the contains method finds the inserted values.
        '''
        for item in self.dataset:
            self.table.add(item)
        for item in self.dataset:
            self.assertIn(item, self.table)

    def test_contains_if_not_contained(self):
        '''
        Ensure the contains method won't find a value that isn't there.
        '''
        for item in self.dataset:
            self.table.add(item)
        for item in self.dataset:
            self.table.remove(item)

        for item in self.dataset:
            self.assertNotIn(item, self.table)

    def test_to_scatter(self):
        '''
        Ensure the scatter lists contain only the inserted elements.
        '''
        for item in self.dataset:
            self.table.add(item)
        scatter = list(self.table.scatter())

        # Convert dataset
        expected = [list() for _ in self.dataset[0]]
        for x in self.dataset:
            for i in range(len(x)):
                expected[i].append(x[i])

        self.assertEqual(len(scatter), len(expected))
        for i in range(len(scatter)):
            self.assertCountEqual(scatter[i], expected[i])

    def test_iter(self):
        '''
        Test the iterator yields all inserted items.
        '''
        for item in self.dataset:
            self.table.add(item)

        self.assertCountEqual(list(self.table), self.dataset)

    def test_never_resizes(self):
        '''
        If a big enough max and min value is given the table should never resize.
        '''
        table = CartesianHashTable(cartesian_tuple, 10, (1000,) * 5, (-1000,) * 5)
        for item in self.dataset:
            table.add(item)
        self.assertEqual(table._resize_count, 0)

    @parameterized.expand([
        [0.3 * i] for i in range(3)
    ])
    def test_finds_neighbor(self, approx):
        '''
        Testing that if a neighbor is present it is found.
        '''
        for item in self.dataset:
            self.table.add(item)

        value = tuple(numpy.random.randint(self.min_value, self.max_value, 5))

        neighbors = [item for item in self.dataset if are_near(
            value, item, approx, cartesian_tuple
        )]
        if neighbors:
            self.assertTrue(self.table.near(value, approx))
        else:
            self.assertFalse(self.table.near(value, approx))

    @parameterized.expand([
        [0.3 * i] for i in range(3)
    ])
    def test_no_neighbors(self, approx):
        '''
        Testing that if no neighbors are present False is returned.
        '''
        for item in self.dataset:
            self.table.add(item)

        value = tuple(numpy.random.randint(self.min_value, self.max_value, 5))

        neighbors = [item for item in self.dataset if are_near(
            value, item, approx, cartesian_tuple
        )]

        for n in neighbors:
            self.table.remove(n)

        self.assertFalse(self.table.near(value, approx))


def test_cartesian_table_repr():
    '''
    Testing the string begins with the class's name.
    '''
    import re
    tc = unittest.TestCase()
    table = CartesianHashTable(origin)
    tc.assertTrue(re.match("CartesianHashTable\\(.*\\)", repr(table)))
    table.add("A value")
    tc.assertTrue(re.match("CartesianHashTable\\(.*\\)", repr(table)))
