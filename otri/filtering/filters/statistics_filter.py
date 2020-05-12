from ..filter import Filter, Stream, Collection
from typing import Sequence, Mapping, Dict
from numbers import Number


class StatisticsFilter(Filter):

    def __init__(self, input_stream: Stream, keys: Sequence):
        '''
        Parameters:
            input_stream : Stream
                The input stream.
            keys : Sequence
                The keys for which to compute the stats.
        '''
        super().__init__(
            [input_stream],
            input_streams_count=1,
            output_streams_count=1
        )
        # Dict like : {callable : dict}
        self.__keys = keys
        self.__ops = dict()
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
            atom = input_iter.__next__()
            for op in self.__ops.keys():
                op(atom)
            self.get_output_stream(0).append(atom)
        # Check that we didn't just pop the last item
        if not input_iter.has_next() and self.get_input_stream(0).is_closed():
            self.get_output_stream(0).close()

    def calc_avg(self):
        '''
        Enable calculating the Average, by enabling both the sum and the count.
        Redundant enabling is a no-op.
        '''
        self.calc_sum()
        self.calc_count()
        return self

    def calc_sum(self):
        '''
        Enable calculating the sum.
        Redundant enabling is a no-op.
        '''
        if self.__sum not in self.__ops.keys():
            self.__ops[self.__sum] = {k: 0 for k in self.__keys}
        return self

    def calc_count(self):
        '''
        Enable counting.
        Redundant enabling is a no-op.
        '''
        if self.__count not in self.__ops.keys():
            self.__ops[self.__count] = {k: 0 for k in self.__keys}
        return self

    def calc_max(self):
        '''
        Enable finding the max.
        Redundant enabling is a no-op.
        '''
        if self.__max not in self.__ops.keys():
            self.__ops[self.__max] = {
                k: float("-inf") for k in self.__keys}
        return self

    def calc_min(self):
        '''
        Enable finding the min.
        Redundant enabling is a no-op.
        '''
        if self.__min not in self.__ops.keys():
            self.__ops[self.__min] = {
                k: float("inf") for k in self.__keys}
        return self

    def get_avg(self)->Mapping:
        '''
        Returns:
            dictionary in the form {key : avg}. Is computed from sum and count values.
        '''
        if self.__count not in self.__ops.keys() or self.__sum not in self.__ops.keys():
            raise RuntimeError("You have not enabled this operation.")
        return {k: self.__ops[self.__sum][k] / self.__ops[self.__count][k] if self.__ops[self.__count][k] != 0 else 0 for k in self.__keys}

    def get_sum(self)->Mapping:
        '''
        Returns:
            A copy of the sum dict. {key : sum}.
        '''
        if self.__sum not in self.__ops.keys():
            raise RuntimeError("You have not enabled this operation.")
        return self.__ops[self.__sum].copy()

    def get_count(self)->Mapping:
        '''
        Returns:
            A copy of the count dict. {key : count}.
        '''
        if self.__count not in self.__ops.keys():
            raise RuntimeError("You have not enabled this operation.")
        return self.__ops[self.__count].copy()

    def get_max(self)->Mapping:
        '''
        Returns:
            A copy of the max dict. {key : max}.
        '''
        if self.__max not in self.__ops.keys():
            raise RuntimeError("You have not enabled this operation.")
        return self.__ops[self.__max].copy()

    def get_min(self)->Mapping:
        '''
        Returns:
            A copy of the min dict. {key : min}.
        '''
        if self.__min not in self.__ops.keys():
            raise RuntimeError("You have not enabled this operation.")
        return self.__ops[self.__min].copy()

    def __sum(self, atom: Mapping):
        '''
        Update the sum state for the keys that are in `atom`.
        '''
        for k in self.__keys:
            if k in atom.keys():
                self.__ops[self.__sum][k] += atom[k]

    def __count(self, atom: Mapping):
        '''
        Update the count state for the keys that are in `atom`.
        '''
        for k in self.__keys:
            if k in atom.keys():
                self.__ops[self.__count][k] += 1

    def __max(self, atom: Mapping):
        '''
        Update the max state for the keys that are in `atom`.
        '''
        for k in self.__keys:
            if k in atom.keys():
                val = self.__ops[self.__max][k]
                self.__ops[self.__max][k] = max(val, atom[k])

    def __min(self, atom: Mapping):
        '''
        Update the min state for the keys that are in `atom`.
        '''
        for k in self.__keys:
            if k in atom.keys():
                val = self.__ops[self.__min][k]
                self.__ops[self.__min][k] = min(val, atom[k])
