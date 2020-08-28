__author__ = "Riccardo De Zen <riccardodezen98@gmail.com>"
__version__ = "1.1"

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

        self._min_axis_span: Real = CartesianHashTable._MIN_VALUE
        '''The minimum value an axis should cover if the only value ever found was 0.'''

        self._count: int = 0
        '''The number of items in the table.'''

        self._dimensions: int = None
        '''The number of dimensions for this table.'''

        self._cell_size: Sequence[Real] = None
        '''Cell size on axes. Always the ceiling of the max supported value / _size'''

        self._max_value: Sequence[Real] = None
        '''The max values found for each table dimension.'''

        self._min_value: Sequence[Real] = None
        '''The min values found for each table dimension.'''

        self._zero: Sequence[int] = None
        '''The index at which zero is situated on the axes.'''

        self._table = None
        '''Table containing the buckets.'''

        self._resize_count = 0

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
        try:
            bucket = self._table[indexes]
        except IndexError:
            raise ValueError("Value not found.")

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

        # Resize and recalculate indexes if needed.
        indexes = self._index(value)
        if self._resize(indexes):
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
        # Max capacity is double the first value inserted.
        self._max_value = [max(c * 2, self._min_axis_span) for c in coords]
        # Min value is double the min inserted value if it's negative.
        self._min_value = [min(c * 2, 0) for c in coords]
        # Divide in blocks that go up to self._max entries
        self._cell_size = [math.ceil(m / self._cell_count) for m in self._max_value]
        # Zero in order to shift negative indexes.
        self._zero = [abs(self._min_value[i] // self._cell_size[i]) for i in range(len(coords))]
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
        '''
        # Divide each coordinate for the right cell size, that is it's index.
        return tuple(c // self._cell_size[i] + self._zero[i]
                     for i, c in enumerate(self.get_coordinates(value)))

    def _resize(self, new_indexes: Sequence[int]) -> bool:
        '''
        Resize the table to twice the size needed to contain the given indexes.

        Parameters:
            new_indexes : Sequence[Real]
                The indexes for which to ensure support.

        Returns:
            True if a resize was due and completed.
            False if no resize was needed.
        '''
        too_big = [(i, cell) for i, cell in enumerate(new_indexes)
                   if cell >= self._cell_count]
        too_small = [(i, cell) for i, cell in enumerate(new_indexes)
                     if cell < 0]
        # All indexes fit.
        if not too_big and not too_small:
            return False

        for i, cell in too_big:
            self._max_value[i] = self._cell_size[i] * cell * 2

        for i, cell in too_small:
            self._min_value[i] = self._cell_size[i] * (cell - self._zero[i]) * 2

        for i in range(len(self._cell_size)):
            self._cell_size[i] = math.ceil(
                (self._max_value[i] - self._min_value[i]) / self._cell_count
            )

        self._zero = [abs(self._min_value[i] // self._cell_size[i])
                      for i in range(self._dimensions)]

        # Redistribute table items.
        old = list(self)
        self._reset_table()
        for item in old:
            self._add(item)

        self._resize_count += 1
        return True

    def _density(self) -> float:
        '''
        Returns:
            float : The density of items in the table. It is _count / (_cell_count ^ _dimensions).
        '''
        return self._count / (self._cell_count ** self._dimensions)

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
