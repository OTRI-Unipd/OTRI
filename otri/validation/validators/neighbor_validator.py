__author__ = "Riccardo De Zen <riccardodezen98@gmail.com>"
__version__ = "1.0"

from .. import ParallelBufferValidator
from ..exceptions import NeighborWarning
from otri.utils.cartesian_hashtable import CartesianHashTable

from typing import TypeVar, Sequence, Mapping, List

K = TypeVar('K')
'''Generic atom key type.'''

# TODO fix the class. The behaviour of execute kinda goes against the principle of _check.


class NeighborValidator(ParallelBufferValidator):

    '''
    Validator aiming to check wether, given K inputs, every atom of every input has at least a
    neighbor in the remaining K-1 inputs. By neighbor we mean an item in a certain time and value
    range.
    '''

    def __init__(self, inputs: Sequence[str], outputs: Sequence[str], time_key: str, time_range: int, limits: Mapping[K, float]):
        '''
        This Validator expects the same number of Stream inputs and outputs.

        Parameters:
            inputs : str
                Names of the inputs. Must be allineated on the time axis.

            outputs : str
                Names of the outputs.

            time_key : str
                The key for the "time" axis. Inputs must be allineated for this key.

            time_range : str
                The range on the time axis where to check for neighbors.

            limits : Mapping
                Mapping of keys to neighbor limits, expressed as a float.
                A atom's neighbor needs to be from 1-limit to 1+limit times the atom on each of
                these keys
        '''
        super().__init__(inputs, outputs)
        self._holding = True
        self._time_key = time_key
        self._time_range = time_range
        self._max_buffer_size = time_range * 2 + 1

        # TODO allow hashtable to use different ranges
        self._limits = limits
        self.__actual_limit = max(abs(x) for x in limits.values())

        def get_coordinates(item):
            return tuple(item[k] for k in self._limits)

        self._table = CartesianHashTable(get_coordinates)

    def _check(self, data: List[Mapping], indexes: List[int]):
        '''
        For each atom in data check the discrepancy with all the successive atoms.

        Parameters:
            data : List[Mapping]
                The atoms retrieved from the inputs.

            indexes : List[int]
                The indexes from which the respective atoms come from.
        '''
        # No neighbors can be found until buffer is full.
        if len(self._hold_buffer) < self._max_buffer_size:
            for atom in data:
                self._table.add(atom)
            return

        # Buffer is full, find neighbors.
        middle = self._hold_buffer[len(self._hold_buffer) // 2]
        for atom in middle:
            if not self._table.near(atom, self.__actual_limit):
                self._add_label(atom, NeighborWarning(atom))

        # Release first batch of atoms and insert new ones.
        for atom in self._buffer_top():
            self._table.remove(atom)
        self._buffer_pop()
        for atom in data:
            self._table.add(atom)
