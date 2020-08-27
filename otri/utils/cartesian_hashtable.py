__author__ = "Riccardo De Zen <riccardodezen98@gmail.com>"
__version__ = "1.0"

from typing import Callable, Generic, TypeVar, Tuple, Sequence, List, Iterable, Iterator
from numbers import Real

import numpy
import math

T = TypeVar('T')
'''Generic type for the CartesianHashTable's contents.'''


class CartesianHashTable(Generic[T], Iterable[T]):

    _BASE_SIZE = 10
    '''Base size, in cells, of the table in every dimension.'''

    _MIN_VALUE = 10
    '''The minimum value an axis should cover if the only value ever found was 0.'''

    '''
    This class holds objects by inserting them into a bidimensional array.
    Each cell in the array represents a square in a bidimensional cartesian plane, hence the name.

    Two objects are stored in a certain square based on their "x" and "y" coordinates, the methods
    that retrieve such coordinates are provided during this object's creation, this will then be the
    `get_coordinates(value)` method.

    The coordinates must always be *positive Real numbers*.
    '''

    def __init__(self, get_coordinates: Callable[[T], Tuple[Real]]):
        '''
        Can hold either mappings or access the coordinates via functions.
        '''
        super().__init__()
        self.get_coordinates: Callable = get_coordinates

        self._cell_count: int = CartesianHashTable._BASE_SIZE
        '''Size in cells, the same for every dimension by default.'''

        self._min_value: Real = CartesianHashTable._MIN_VALUE
        '''The minimum value an axis should cover if the only value ever found was 0.'''

        self._count: int = 0
        '''The number of items in the table.'''

        self._dimensions: int = None
        '''The number of dimensions for this table.'''

        self._cell_size: Sequence[Real] = None
        '''Cell size on axes. Always the ceiling of the max supported value / _size'''

        self._max_value: Sequence[Real] = None
        '''The max values found for each table dimension.'''

        self._table = None
        '''Table containing the buckets.'''

    def add(self, value: T):
        '''
        Add the given value to the table, in the appropriate square.

        Parameters:
            value : T
                The value to add, must be of type T, or at least answer coherently to the `get_x`
                and `get_y` constructor parameters.
        '''
        self._add(value)
        self._count += 1

    def remove(self, value: T):
        '''
        Remove a certain value if its present.

        Parameters:
            value : T
                Value to remove from the table.

        Raises:
            `ValueError` if the element is not in the table.
        '''
        if self._table is None:
            raise ValueError("Empty table.")

        indexes = self._index(value)
        bucket = self._table[indexes]

        if bucket is None:
            raise ValueError("Value not found.")

        bucket.remove(value)
        self._count -= 1

    def size(self) -> int:
        '''
        Returns:
            int : The number of items in the Table.
        '''
        return self._count

    def get_coordinates(self, value: T) -> Tuple[Real]:
        '''
        Method must be provided during object creation.

        Parameters:
            value : T
                The value for which to find the coordinates.

        Returns:
            A Tuple containing Real numbers, the coordinates in the cartesian space.
            The length of this tuple is used when creating the table, so it should always be the
            same.
        '''
        raise NotImplementedError("This method must be provided when the object is created.")

    def scatter(self) -> Tuple[List[T]]:
        '''
        Returns:
            A tuple of lists, containing the coordinates of all items in the Table, as
            if you saw them on a scatter plot.
            If the data is bi-dimensional it is useful for visualizing with matplotlib.
        '''
        coords = tuple([list() for _ in range(self._dimensions)])
        for item in self:
            for i, c in enumerate(self.get_coordinates(item)):
                coords[i].append(c)
        return coords

    def near(self, value, approx=0):
        '''
        True if value is in the Table. Find its square and look for it.
        If approx different than 0 find all the possible squares in two dimensions where the
        value might be. Look for it in those squares.
        '''
        pass

    def _add(self, value: T):
        '''
        Actual add method implementation. Used to add without increasing table size.
        '''
        # Table setup.
        if self._table is None:
            self._setup(value)

        # Ensure resizing if needed.
        try:
            indexes = self._index(value)
        except IndexError:
            self._resize()
            indexes = self._index(value)

        if self._table[indexes] is None:
            self._table[indexes] = list()

        self._table[indexes].append(value)

    def _setup(self, value: T):
        '''
        Calibrates some parameters for the table's first use.

        Parameters:
            value : T
                The first value to insert.
        '''
        coords = self.get_coordinates(value)
        # Max capacity is double the newfound value.
        self._max_value = [max(c * 2, self._min_value) for c in coords]
        # Divide in blocks that go up to self._max entries
        self._cell_size = [math.ceil(m / self._cell_count) for m in self._max_value]
        # The dimensions are as many as the coordinates.
        self._dimensions = len(coords)
        # Initialize the table.
        self._reset_table()

    def _reset_table(self):
        '''
        Initialize the array used to contain the data. Requires `self._dimensions` and
        `self._size` to be set.

        Raises:
            TypeError : if `self._dimensions` is not an integer.
            AttributeError : if `self._dimensions` or `self._size` are not defined.
        '''
        self._table = numpy.empty((self._cell_count,) * self._dimensions, dtype=object)

    def _index(self, value: T) -> Tuple[int]:
        '''
        Return the indexes of a certain value. These are not the Real coordinates returned by
        the method the user passed, these are the actual indexes for the table cell to which the
        value belongs.

        Parameters:
            value : T
                The value for which to find the coordinates.

        Returns:
            The coordinates (table cell) for the given value.

        Raises:
            IndexError : if the resulting index for the item won't fit in the table and resizing is
            due. Will update _max with the new suggested limits before raising.
        '''
        # First insertion: Never defined block size and max values.
        coords = self.get_coordinates(value)
        must_resize = False
        indexes = list()
        for i, c in enumerate(coords):
            index = c // self._cell_size[i]
            # New max will be twice the out of bounds value.
            if c > self._max_value[i]:
                self._max_value[i] = 2 * c
                must_resize = True
            indexes.append(index)

        if must_resize:
            raise IndexError

        return tuple(indexes)

    def _resize(self):
        '''
        Resize the table. Requires `self._max` to be set and a Sequence.
        '''
        old = list(self)
        # Divide in blocks that go up to self._max entries
        self._cell_size = [math.ceil(m / self._cell_count) for m in self._max_value]
        # Re-initialize the table.
        self._reset_table()

        # Readd the items.
        for item in old:
            self._add(item)

    def __iter__(self) -> Iterator:
        '''
        This is a Generator method, loops through the table yielding items one at a time.

        Returns:
            Iterator yielding items one at a time.

        Raises:
            `StopIteration` when the end of the table has been reached.
        '''
        with numpy.nditer(self._table, flags=['refs_ok']) as iterator:
            for bucket in iterator:
                if bucket.tolist() is None:
                    continue

                for item in bucket.tolist():
                    yield item

    def __contains__(self, item: T) -> bool:
        '''
        Overriding for `in` keyword.

        Parameters:
            item : T
                The item to find in the table.

        Returns:
            True if the item is in the table, False otherwise.
        '''
        if self._table is None:
            return False
        bucket = self._table[self._index(item)]
        return item in bucket if bucket is not None else False
