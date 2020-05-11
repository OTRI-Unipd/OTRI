from ..filter import Filter, Stream, Collection
from typing import Sequence, Mapping, Dict
from numbers import Number


class AverageFilter(Filter):
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
        super().__init__([input_stream],
                         input_streams_count=1, output_streams_count=1)
        self.__cnt = {k: 0 for k in keys}
        self.__sum = {k: 0 for k in keys}
        self.__avg = {k: 0 for k in keys}
        self.__input_iter = input_stream.__iter__()

    def execute(self):
        '''
        Pops a single atom, reads the fields in the `keys` init parameter, updates the state
        and outputs the atom, unmodified.
        '''
        if self.get_output_stream(0).is_closed():
            return
        input_iter = self.__input_iter
        if input_iter.has_next():
            item = input_iter.__next__()
            self.__update_state(item)
            self.get_output_stream(0).append(item)
        # Check that we didn't just pop the last item
        if not input_iter.has_next() and self.get_input_stream(0).is_closed():
            self.get_output_stream(0).close()

    def get_avgs(self) -> Dict:
        '''
        Returns:
            A copy of the dict containing all the averages, for each key.
        '''
        return self.__avg.copy()

    def get_sums(self) -> Dict:
        '''
        Returns:
            A copy of the dict containing all the sums, for each key.
        '''
        return self.__sum.copy()

    def get_counts(self) -> Dict:
        '''
        Returns:
            A copy of the dict containing all the counts, for each key. Meaning how many atoms had that key.
        '''
        return self.__cnt.copy()

    def __update_state(self, item: Mapping):
        '''
        Parameters:
            item : Mapping
                The atom to use when updating the state.
                It is not necessary for it to have the keys.
        '''
        for k in self.__cnt.keys():
            if k in item.keys() and isinstance(item[k], Number):
                self.__cnt[k] += 1
                self.__sum[k] += item[k]
                self.__avg[k] = self.__sum[k] / self.__cnt[k]
