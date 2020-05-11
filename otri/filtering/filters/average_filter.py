from ..filter import Filter, Stream, Collection
from .integrator_filter import IntegratorFilter
from typing import Sequence, Mapping, Dict
from numbers import Number


class AverageFilter(IntegratorFilter):
    '''
    Input: One Stream
    Output: One Stream
    Computes the average and the sum of the atoms that pass through.
    '''

    def __init__(self, input_stream: Stream, keys: Sequence):
        '''
        Parameters:
            input_stream : Stream
                A single Stream of atoms, which we assume to be mappings of some sort.
            keys : Sequence
                The keys to compute the average of. Values corresponding to these keys
                should be numbers.
        '''
        super().__init__(input_stream, keys)
        self.__keys = keys

    def get_avgs(self) -> Dict:
        '''
        Returns:
            A copy of the dict containing all the averages, for each key.
        '''
        avg_dict = dict()
        sums = self.get_sums()
        counts = self.get_counts()
        for k in self.__keys:
            avg_dict[k] = sums[k] / counts[k]
        return avg_dict
