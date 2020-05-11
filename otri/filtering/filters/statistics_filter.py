from ..filter import Filter, Stream, Collection
from typing import Sequence, Mapping, Dict
from numbers import Number


class StatisticsFilter(Filter):

    def __init__(self, input_stream: Stream, keys: Sequence):
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
        self.calc_sum()
        self.calc_count()
        return self

    def calc_sum(self):
        if self.__sum not in self.__ops.keys():
            self.__ops[self.__sum] = {k: 0 for k in self.__keys}
        return self

    def calc_count(self):
        if self.__count not in self.__ops.keys():
            self.__ops[self.__count] = {k: 0 for k in self.__keys}
        return self

    def calc_max(self):
        if self.__max not in self.__ops.keys():
            self.__ops[self.__max] = {
                k: float("-inf") for k in self.__keys}
        return self

    def calc_min(self):
        if self.__min not in self.__ops.keys():
            self.__ops[self.__min] = {
                k: float("inf") for k in self.__keys}
        return self

    def get_avg(self):
        if self.__count not in self.__ops.keys() or self.__sum not in self.__ops.keys():
            raise RuntimeError("You have not enabled this operation.")
        return {k: self.__ops[self.__sum][k] / self.__ops[self.__count][k] for k in self.__keys}

    def get_sum(self):
        if self.__sum not in self.__ops.keys():
            raise RuntimeError("You have not enabled this operation.")
        return self.__ops[self.__sum].copy()

    def get_count(self):
        if self.__count not in self.__ops.keys():
            raise RuntimeError("You have not enabled this operation.")
        return self.__ops[self.__count].copy()

    def get_max(self):
        if self.__max not in self.__ops.keys():
            raise RuntimeError("You have not enabled this operation.")
        return self.__ops[self.__max].copy()

    def get_min(self):
        if self.__min not in self.__ops.keys():
            raise RuntimeError("You have not enabled this operation.")
        return self.__ops[self.__min].copy()

    def __sum(self, atom: Mapping):
        for k in self.__keys:
            if k in atom.keys():
                self.__ops[self.__sum][k] += atom[k]

    def __count(self, atom: Mapping):
        for k in self.__keys:
            if k in atom.keys():
                self.__ops[self.__count][k] += 1

    def __max(self, atom: Mapping):
        for k in self.__keys:
            if k in atom.keys():
                val = self.__ops[self.__max][k]
                self.__ops[self.__max][k] = max(val, atom[k])

    def __min(self, atom: Mapping):
        for k in self.__keys:
            if k in atom.keys():
                val = self.__ops[self.__min][k]
                self.__ops[self.__min][k] = min(val, atom[k])