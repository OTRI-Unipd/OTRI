__author__ = "Riccardo De Zen <riccardodezen98@gmail.com>"
__version__ = "1.1"

from typing import Callable, Generic, TypeVar, Tuple, Sequence, List, Iterable, Iterator
from numbers import Real

import numpy
import math

T = TypeVar('T')
'''Generic type for the CartesianHashTable's contents.'''


class CartesianHashTable(Generic[T], Iterable[T]):

    _DEFAULT_CELL_COUNT = 10
    '''Base size, in cells, of the table in every dimension.'''

    _MIN_AXIS_SPAN = 10
    '''The minimum value an axis should cover if the only value ever found was 0.'''

    '''
    This class holds objects by inserting them into a bidimensional array.
    Each cell in the array represents a square in a bidimensional cartesian plane, hence the name.

    Two objects are stored in a certain square based on their "x" and "y" coordinates, the methods
    that retrieve such coordinates are provided during this object's creation, this will then be the
    `get_coordinates(value)` method.

    The coordinates must **always** be `Real` numbers.
    '''

    def __init__(self, get_coordinates: Callable[[T], Tuple[Real]], cell_count: int = 10,
                 max_values: Sequence[Real] = None, min_values: Sequence[Real] = None):
        '''
        Parameters:
            get_coordinates : Callable[[T], Tuple[Real]]
                The method that computes the Real coordinates from the T objects.

            cell_count : int
                Number of cells on each axis. Higher means higher precision, but longer resizing
                times.

            max_values : Sequence[Real]
                An estimate of the maximum values that will be found on each axis. Using this
                calibrates the table on such values, reducing resizes.

            min_values : Sequence[Real]
                An estimate of the minimum values that will be found on each axis. Using this
                calibrates the table on such values, reducing resizes.
        '''
        super().__init__()
        self.get_coordinates: Callable = get_coordinates

        self._cell_count: int = cell_count or CartesianHashTable._DEFAULT_CELL_COUNT
        '''Size in cells, the same for every dimension by default.'''

        self._min_axis_span: Real = CartesianHashTable._MIN_AXIS_SPAN
        '''The minimum value an axis should cover if the only value ever found was 0.'''

        self._count: int = 0
        '''The number of items in the table.'''

        self._dimensions: int = None
        '''The number of dimensions for this table.'''

        self._cell_size: Sequence[Real] = None
        '''Cell size on axes. Always the ceiling of the max supported value / _size'''

        self._max_value: Sequence[Real] = list(max_values) if max_values else None
        '''The max values found for each table dimension.'''

        self._min_value: Sequence[Real] = list(min_values) if min_values else None
        '''The min values found for each table dimension.'''

        self._zero: Sequence[int] = None
        '''The index at which zero is situated on the axes.'''

        self._table = None
        '''Table containing the buckets.'''

        self._initialized_buckets = list()

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

    def near(self, value: T, approx: Real = 0) -> bool:
        '''
        Find if there is a value in the table, with a certain aproximation on the coordinates.

        Parameters:
            value : T
                The value to find.

            approx : Real
                The approximation interval, multiplies value on all axis. Used in absolute value.

        Returns:
            bool : wether something between (1 - `approx`) * `value` and (1 + `approx`) * `value`.
            For example: if approx = 2 and there is something in the table between -value and
            3*value then True will be returned.
        '''
        if approx == 0:
            return value in self

        approx = abs(approx)
        coords = self.get_coordinates(value)
        # We don't care about looking before 0
        left_coords = tuple((1 - approx) * c for c in coords)
        left = tuple(
            max(c // self._cell_size[i] + self._zero[i], 0) for i, c in
            enumerate(left_coords)
        )
        # We don't care about looking out of size.
        right_coords = tuple((1 + approx) * c for c in coords)
        right = tuple(
            min(c // self._cell_size[i] + self._zero[i], self._cell_count) for i, c in
            enumerate(right_coords)
        )

        def in_range(bucket):
            '''Return wether bucket is in the wanted range.'''
            for i, c in enumerate(bucket):
                if not left[i] <= c <= right[i]:
                    return False
            return True

        buckets = list(filter(in_range, self._initialized_buckets))
        for b in buckets:
            for item in self._table[b]:
                coords = self.get_coordinates(item)
                if self._between(left_coords, coords, right_coords):
                    return True
        return False

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
            self._initialized_buckets.append(indexes)

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
        self._max_value = self._max_value or [max(c * 2, self._min_axis_span) for c in coords]
        # Min value is double the min inserted value if it's negative.
        self._min_value = self._min_value or [min(c * 2, 0) for c in coords]
        # Divide in blocks that go up to self._max entries
        self._cell_size = [math.ceil((self._max_value[i] - self._min_value[i]) / self._cell_count)
                           for i in range(len(coords))]
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
        self._initialized_buckets = list()
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
            self._cell_size[i] = math.ceil(
                (self._max_value[i] - self._min_value[i]) / self._cell_count
            )

        for i, cell in too_small:
            self._min_value[i] = self._cell_size[i] * (cell - self._zero[i]) * 2
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

    # ! MISSING SPECS
    def _between(self, left, item, right):
        for L, X, R in zip(left, item, right):
            if not L <= X <= R:
                return False
        return True

    def __iter__(self) -> Iterator:
        '''
        This is a Generator method, loops through the table yielding items one at a time.

        Returns:
            Iterator yielding items one at a time.

        Raises:
            `StopIteration` when the end of the table has been reached.
        '''
        for indexes in self._initialized_buckets:
            for item in self._table[indexes]:
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

    def __repr__(self) -> str:
        '''
        Returns:
            Canonical string representation.
        '''
        if self._table is None:
            return self.__class__.__name__ + "(Empty)"
        return repr(self._table).replace("array", self.__class__.__name__, 1)

    def __str__(self) -> str:
        '''
        Returns:
            String representation.
        '''
        return str(self._table)
